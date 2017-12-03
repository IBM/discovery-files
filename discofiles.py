#!/usr/bin/env python3

import argparse
from hashlib import sha1
import json
import os
import queue
import sys
import threading
import time

from pmap import pmap
from watson_developer_cloud import DiscoveryV1


class Args:
    def __init__(self, creds):
        self.url = creds.get("url")
        self.username = creds.get("username")
        self.password = creds.get("password")
        self.version = creds.get("version", "2017-09-01")
        self.environment_id = creds.get("environment_id")
        self.collection_id = creds.get("collection_id")
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
                with open(this_path, "rb") as f:
                    self.discovery.update_document(self.environment_id,
                                                   self.collection_id,
                                                   this_sha1,
                                                   f)
            except:
                exception = sys.exc_info()[1]
                exception_args = exception.args[0]
                if isinstance(exception_args, str):
                    parsed_string = exception_args[-3:]
                    if parsed_string == "429":
                        self.wait_until = time.perf_counter() + 5
                        self.queue.put(item)
                    else:
                        print("Failing {} due to {}"
                              .format(this_path, exception_args))
                        self.counts[parsed_string] = self.counts.get(
                            parsed_string, 0) + 1
                else:
                    print("Failing {} due to {}"
                          .format(this_path, exception_args))
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
    for environment in discovery.get_environments()["environments"]:
        if not environment["read_only"]:
            return environment["environment_id"]


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
                                   {"count": chunk_size,
                                    "filter": "extracted_metadata.sha1::"
                                              + prefix + "*",
                                    "return": "extracted_metadata.sha1"})
        if response["matching_results"] > chunk_size:
            return prefix
        else:
            return [item["extracted_metadata"]["sha1"]
                    for item in response["results"]]

    prefixes_to_process = [""]
    while prefixes_to_process:
        prefix = prefixes_to_process.pop(0)
        prefixes = [prefix + letter for letter in alphabet]
        # `pmap` here does the requests to Discovery concurrently to save time.
        results = pmap(maybe_some_sha1s, prefixes, threads=len(prefixes))
        for result in results:
            if isinstance(result, list):
                sha1s += result
            else:
                prefixes_to_process.append(result)

    return sha1s


def main(args):
    discovery = DiscoveryV1(args.version,
                            url=args.url,
                            username=args.username,
                            password=args.password)
    args.environment_id = writable_environment_id(discovery)
    collections = discovery.list_collections(
        args.environment_id)["collections"]
    if len(collections) == 1:
        args.collection_id = collections[0]["collection_id"]

    if not args.collection_id:
        if collections:
            print("Error: multiple collections found. Please specify which one to use.")
        else:
            print("Error: no target collection found. Please create a collection.")
        exit(1)

    work = Worker(discovery, args.environment_id,
                  args.collection_id)

    index_list = existing_sha1s(discovery,
                                args.environment_id,
                                args.collection_id)
    indexed = set(index_list)
    count_ignore = 0
    count_ingest = 0
    for path in args.paths:
        for root, _dirs, files in os.walk(path):
            for name in files:

                this_path = os.path.join(root, name)
                with open(this_path, "rb") as this_file:
                    content = this_file.read()
                    this_sha1 = sha1(content).hexdigest()
                if this_sha1 in indexed:
                    count_ignore += 1
                else:
                    count_ingest += 1
                    work.put_in_queue((this_path, this_sha1))

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
                        help='JSON file containing Discovery service credentials; default: "credentials.json"')
    parser.add_argument("-collection_id",
                        help="Discovery collection_id; defaults to an existing collection, when there is only one.")

    parsed = parser.parse_args()
    with open(parsed.json) as creds_file:
        args = Args(json.load(creds_file))
    args.paths = parsed.path
    if parsed.collection_id:
        args.collection_id = parsed.collection_id
    return args


if __name__ == "__main__":
    main(parse_command_line())
