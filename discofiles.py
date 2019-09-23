#!/usr/bin/env python3
"""
A simple tool to send files into Watson Discovery, with simple retry.
"""

import argparse
import concurrent.futures
import json
import os
import queue
import sys
from dataclasses import dataclass
from hashlib import sha1
from mimetypes import guess_type
from typing import Dict, List, Optional, Set, Tuple

from ibm_watson import DiscoveryV1

CONCURRENCY = 16


@dataclass
class Args:
    """
    Class for holding command line arguments.
    """

    paths: List[str]
    collection_id: Optional[str] = None
    dry_run: bool = False
    environment_id: Optional[str] = None
    iam_api_key: Optional[str] = None
    password: Optional[str] = None
    url: Optional[str] = None
    username: Optional[str] = None
    version: str = "2019-09-23"

    def init_from_dict(self, credentials: Dict[str, str]):
        """
        Initialize many instance variables from a dict
        (presumed to have been parsed from JSON).
        """
        self.url = credentials.get("url")
        self.username = credentials.get("username")
        self.password = credentials.get("password")
        self.version = credentials.get("version", self.version)
        self.environment_id = credentials.get("environment_id")
        self.collection_id = credentials.get("collection_id")
        self.iam_api_key = credentials.get("apikey")


@dataclass(frozen=True)
class Target:
    """
    Target discovery collection information.
    """

    discovery: DiscoveryV1
    environment_id: str
    collection_id: str


def send_file(target: Target, this_path: str, this_sha1: str) -> None:
    """
    Send a single file into Discovery.
    """
    try:
        mime = guess_type(this_path)[0]
        if not mime:
            mime = "application/octet-stream"
        with open(this_path, "rb") as file_handle:
            target.discovery.update_document(
                environment_id=target.environment_id,
                collection_id=target.collection_id,
                document_id=this_sha1,
                file=file_handle,
                file_content_type=mime,
            )
    except:
        print("Failing {} due to {}".format(this_path, sys.exc_info()[1]))


def writable_environment_id(discovery: DiscoveryV1) -> str:
    """
    Return the environment id of the writable environment, if any.
    """
    for environment in discovery.list_environments().get_result()["environments"]:
        if not environment["read_only"]:
            return environment["environment_id"]
    print(
        "Error: no writable environment found. "
        "Please create a bring your own data environment."
    )
    exit(1)
    return ""  # pylint complains! Ha!


def pmap(action, input_iterable) -> list:
    """
    Simple concurrent map function that uses threads.
    Each element in the input list will be processed in its own thread.

    This is only useful for mapping over a function that does I/O,
    since Python has a Global Interpeter Lock (GIL) that prevents
    code from running concurrently on multiple CPUs.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [executor.submit(action, item) for item in input_iterable]
        return [future.result() for future in futures]


def existing_sha1s(target: Target) -> List[str]:
    """
    Return a list of all of the extracted_metadata.sha1 values found in a
    Watson Discovery collection.

    The arguments to this function are:
    discovery      - an instance of DiscoveryV1
    environment_id - an environment id found in your Discovery instance
    collection_id  - a collection id found in the environment above
    """
    sha1s: List[str] = []
    alphabet = "0123456789abcdef"  # Hexadecimal digits, lowercase
    chunk_size = 10000

    def maybe_some_sha1s(prefix):
        """
        A helper function that does the query and returns either:
        1) A list of SHA1 values
        2) The `prefix` that needs to be subdivided into more focused queries
        """
        response = target.discovery.query(
            environment_id=target.environment_id,
            collection_id=target.collection_id,
            count=chunk_size,
            filter="extracted_metadata.sha1::" + prefix + "*",
            return_fields="extracted_metadata.sha1",
        )
        result = response.get_result()
        if result["matching_results"] > chunk_size:
            return prefix
        return [item["extracted_metadata"]["sha1"] for item in result["results"]]

    prefixes_to_process = [""]
    while prefixes_to_process:
        prefix = prefixes_to_process.pop(0)
        prefixes = [prefix + letter for letter in alphabet]
        # `pmap` here does the requests to Discovery concurrently to save time.
        for result in pmap(maybe_some_sha1s, prefixes):
            if isinstance(result, list):
                sha1s += result
            else:
                prefixes_to_process.append(result)

    return sha1s


def do_one_file(
    file_path: str, indexed: Set[str], work_q: queue.Queue, dry_run: bool
) -> Tuple[int, int]:
    """
    Read a file and conditionally add it to the queue of files to send to Discovery.
    """
    _, ext = os.path.splitext(file_path)
    if ext == ".csv":
        print("CSV files are not yet supported. Ignoring", file_path)
        return (0, 1)
    if ext == ".tar":
        print("Tar files are not yet supported. Ignoring", file_path)
        return (0, 1)
    if ext == ".zip":
        print("Zip files are not yet supported. Ignoring", file_path)
        return (0, 1)

    with open(file_path, "rb") as this_file:
        content = this_file.read()
        this_sha1 = sha1(content).hexdigest()

    if this_sha1 in indexed:
        return (0, 1)

    if dry_run:
        print("dry run", this_sha1, "path", file_path)
    else:
        work_q.put((file_path, this_sha1))
    return (1, 0)


def walk_paths(
    paths: List[str], index_list: List[str], work_q: queue.Queue, dry_run: bool
) -> None:
    """
    Walk through each path given on the command line, handling each file in turn.
    """
    count_ignore = 0
    count_ingest = 0
    indexed = set(index_list)
    for path in paths:
        if os.path.isfile(path):
            ingested, ignored = do_one_file(path, indexed, work_q, dry_run)
            count_ingest += ingested
            count_ignore += ignored
        else:
            for root, _dirs, files in os.walk(path):
                for name in files:
                    ingested, ignored = do_one_file(
                        os.path.join(root, name), indexed, work_q, dry_run
                    )
                    count_ingest += ingested
                    count_ignore += ignored

    work_q.put(None)

    print(
        "Ignored",
        count_ignore,
        "file(s), because they were found in collection.",
        "\nIngesting",
        count_ingest,
        "file(s).",
    )


def main(args: Args) -> None:
    """
    Main program. Reads files from disk and optionally sends each file to Discovery
    """
    discovery = DiscoveryV1(
        args.version,
        url=args.url,
        username=args.username,
        password=args.password,
        iam_apikey=args.iam_api_key,
    )
    args.environment_id = writable_environment_id(discovery)
    collections = discovery.list_collections(args.environment_id).get_result()[
        "collections"
    ]
    if len(collections) == 1:
        args.collection_id = collections[0]["collection_id"]

    if not args.collection_id:
        if collections:
            print("Error: multiple collections found. Please specify which one to use.")
        else:
            print("Error: no target collection found. Please create a collection.")
        exit(1)
    target = Target(discovery, args.environment_id, args.collection_id)

    index_list = existing_sha1s(target)
    work_q: queue.Queue = queue.Queue(CONCURRENCY)
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY + 1) as executor:
        executor.submit(walk_paths, args.paths, index_list, work_q, args.dry_run)
        futures = set()
        item = work_q.get()
        while item:
            futures.add(executor.submit(send_file, target, *item))
            while len(futures) >= CONCURRENCY:
                # We're at our desired concurrency, wait for something to complete.
                _, futures = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )
            item = work_q.get()


def parse_command_line() -> Args:
    """
    Parse the user's command line.
    """
    parser = argparse.ArgumentParser(description="Send files into Watson Discovery")
    parser.add_argument(
        "path", nargs="+", help="File or directory of files to send to Discovery"
    )
    parser.add_argument(
        "-json",
        default="credentials.json",
        help="JSON file containing Discovery service credentials;"
        ' default: "credentials.json"',
    )
    parser.add_argument(
        "-collection_id",
        help="Discovery collection_id;"
        " defaults to an existing collection, when there is only one.",
    )
    parser.add_argument(
        "-dry_run",
        action="store_true",
        help="Don't ingest anything; just report what would be ingested",
    )

    parsed = parser.parse_args()
    args = Args(dry_run=parsed.dry_run, paths=parsed.path)
    with open(parsed.json) as creds_file:
        args.init_from_dict(json.load(creds_file))
    if parsed.collection_id:
        args.collection_id = parsed.collection_id
    return args


if __name__ == "__main__":
    main(parse_command_line())
