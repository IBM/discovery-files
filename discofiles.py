#!/usr/bin/env python3

import argparse
from hashlib import sha1
import json
from mimetypes import guess_type
import os
import queue
import sys
import threading
import time

from ibm_watson import DiscoveryV1, ApiException


class Args:
    def __init__(self, creds):
        self.url = creds.get("url")
        self.username = creds.get("username")
        self.password = creds.get("password")
        self.version = creds.get("version", "2017-09-01")
        self.environment_id = creds.get("environment_id")
        self.collection_id = creds.get("collection_id")
        self.iam_api_key = creds.get("apikey")
        self.dry_run = False
        self.paths = []

    def __str__(self):
        """Formatted string of our variables"""
        return """url            {}
username       {}
password       {}
version        {}
environment_id {}
collection_id  {}
paths          {}""".format(self.url,
                            self.username,
                            self.password,
                            self.version,
                            self.environment_id,
                            self.collection_id,
                            self.paths)


class Worker:
    def __init__(self, discovery, environment_id, collection_id):
        self.counts = {}
        self.discovery = discovery
        self.environment_id = environment_id
        self.collection_id = collection_id
        self.queue = queue.Queue()
        self.thread_count = 64
        self.wait_until = 0
        for _ in range(self.thread_count):
            threading.Thread(target=self.worker, daemon=True).start()

    def worker(self):
        item = self.queue.get()

        while item:
            if item is None:
                break

            # Pause this thread when we need to back off pushing
            wait_for = self.wait_until - time.perf_counter()
            if wait_for > 0:
                time.sleep(wait_for)

            this_path, this_sha1 = item
            try:
                mime = guess_type(this_path)[0]
                if not mime:
                    mime = "application/octet-stream"
                with open(this_path, "rb") as f:
                    self.discovery.update_document(self.environment_id,
                                                   self.collection_id,
                                                   this_sha1,
                                                   file=f,
                                                   file_content_type=mime)
            except:
                exception = sys.exc_info()[1]
                if isinstance(exception, ApiException):
                    if exception.code == 429:
                        self.wait_until = time.perf_counter() + 5
                        self.queue.put(item)
                    else:
                        print("Failing {} due to {}"
                              .format(this_path, exception))
                        self.counts[str(exception.code)] = self.counts.get(
                            str(exception.code), 0) + 1
                else:
                    print("Failing {} due to {}"
                          .format(this_path, exception))
                    self.counts["UNKNOWN"] = self.counts.get("UNKNOWN", 0) + 1

            self.queue.task_done()
            item = self.queue.get()

    def put_in_queue(self, item):
        """Push an item into our work queue."""
        self.queue.put(item)

    def finish(self):
        """Block until all tasks are done then return runtime."""
        self.queue.join()
        for _ in range(self.thread_count):
            self.queue.put(None)
        for code, count in self.counts.items():
            print("The error code", code, "was returned", count, "time(s).")


def writable_environment_id(discovery):
    for environment in discovery.list_environments().get_result()["environments"]:
        if not environment["read_only"]:
            return environment["environment_id"]


def pmap_helper(fn, output_list, input_list, i):
    output_list[i] = fn(input_list[i])


def pmap(fn, input):
    """
    Very simple parallel map function that uses threads.
    Each element in the input list will be processed in its own thread.
    This means the input list had better not be too large.

    This is only useful for mapping over a function that does I/O,
    since Python has a Global Interpeter Lock (GIL) that prevents
    code from running concurrently on multiple CPUs.

    The existing `pmap` libraries I found are process based, which
    shouldn't be a problem for my use. The trouble is they leak processes
    (or maybe threads; I don't really know what they do under the covers;
    I just know they leak and eventually crash.)
    """
    input_list = list(input)
    output_list = [None for _ in range(len(input_list))]
    threads = [threading.Thread(target=pmap_helper,
                                args=(fn, output_list, input_list, i),
                                daemon=True)
               for i in range(len(input_list))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return output_list


def existing_sha1s(discovery,
                   environment_id,
                   collection_id):
    """
    Return a list of all of the extracted_metadata.sha1 values found in a
    Watson Discovery collection.

    The arguments to this function are:
    discovery      - an instance of DiscoveryV1
    environment_id - an environment id found in your Discovery instance
    collection_id  - a collection id found in the environment above
    """
    sha1s = []
    alphabet = "0123456789abcdef"   # Hexadecimal digits, lowercase
    chunk_size = 10000

    def maybe_some_sha1s(prefix):
        """
        A helper function that does the query and returns either:
        1) A list of SHA1 values
        2) The `prefix` that needs to be subdivided into more focused queries
        """
        response = discovery.query(environment_id,
                                   collection_id,
                                   count=chunk_size,
                                   filter="extracted_metadata.sha1::" + prefix + "*",
                                   return_fields="extracted_metadata.sha1")
        result = response.get_result()
        if result["matching_results"] > chunk_size:
            return prefix
        else:
            return [item["extracted_metadata"]["sha1"]
                    for item in result["results"]]

    prefixes_to_process = [""]
    while prefixes_to_process:
        prefix = prefixes_to_process.pop(0)
        prefixes = [prefix + letter for letter in alphabet]
        # `pmap` here does the requests to Discovery concurrently to save time.
        results = pmap(maybe_some_sha1s, prefixes)
        for result in results:
            if isinstance(result, list):
                sha1s += result
            else:
                prefixes_to_process.append(result)

    return sha1s


def do_one_file(file_path, work, indexed, dry_run):
    _, ext = os.path.splitext(file_path)
    if ext == ".csv":
        print("CSV files are not yet supported. Ignoring", file_path)
        return (0, 1)
    elif ext == ".tar":
        print("Tar files are not yet supported. Ignoring", file_path)
        return (0, 1)
    elif ext == ".zip":
        print("Zip files are not yet supported. Ignoring", file_path)
        return (0, 1)

    with open(file_path, "rb") as this_file:
        content = this_file.read()
        this_sha1 = sha1(content).hexdigest()

    if this_sha1 in indexed:
        return (0, 1)
    else:
        if dry_run:
            print("dry run", this_sha1, "path", file_path)
        else:
            work.put_in_queue((file_path, this_sha1))
        return (1, 0)


def main(args):
    discovery = DiscoveryV1(args.version,
                            url=args.url,
                            username=args.username,
                            password=args.password,
                            iam_apikey=args.iam_api_key)
    args.environment_id = writable_environment_id(discovery)
    collections = discovery.list_collections(
        args.environment_id).get_result()["collections"]
    if len(collections) == 1:
        args.collection_id = collections[0]["collection_id"]

    if not args.collection_id:
        if collections:
            print("Error: multiple collections found. Please specify which one to use.")
        else:
            print("Error: no target collection found. Please create a collection.")
        exit(1)

    work = Worker(discovery, args.environment_id, args.collection_id)

    index_list = existing_sha1s(discovery,
                                args.environment_id,
                                args.collection_id)
    indexed = set(index_list)
    count_ignore = 0
    count_ingest = 0
    for path in args.paths:
        if os.path.isfile(path):
            ingested, ignored = do_one_file(path, work, indexed, args.dry_run)
            count_ingest += ingested
            count_ignore += ignored
        else:
            for root, _dirs, files in os.walk(path):
                for name in files:
                    ingested, ignored = do_one_file(os.path.join(root, name),
                                                    work,
                                                    indexed,
                                                    args.dry_run)
                    count_ingest += ingested
                    count_ignore += ignored

    print("Ignored", count_ignore, "file(s), because they were found in collection.",
          "\nIngesting", count_ingest, "file(s).")

    work.finish()


def parse_command_line():
    parser = argparse.ArgumentParser(
        description="Send files into Watson Discovery")
    parser.add_argument("path",
                        nargs="+",
                        help="File or directory of files to send to Discovery")
    parser.add_argument("-json",
                        default="credentials.json",
                        help='JSON file containing Discovery service credentials;'
                             ' default: "credentials.json"')
    parser.add_argument("-collection_id",
                        help="Discovery collection_id;"
                             " defaults to an existing collection, when there is only one.")
    parser.add_argument("-dry_run",
                        action="store_true",
                        help="Don't ingest anything; just report what would be ingested")

    parsed = parser.parse_args()
    with open(parsed.json) as creds_file:
        args = Args(json.load(creds_file))
    args.paths = parsed.path
    if parsed.collection_id:
        args.collection_id = parsed.collection_id
    args.dry_run = parsed.dry_run
    return args


if __name__ == "__main__":
    main(parse_command_line())
