#!/usr/bin/env python3

import argparse
from hashlib import sha1
import json
import os
import queue
import sys
import threading
import time


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


def set_of_existing_sha1s(discovery,
                          environment_id,
                          collection_id):
    set_of_sha1s = set()
    chuck_size = 10000
    # Fake result set to get us into the loop below
    results = {"matching_results": chuck_size + 1,
               "results": [{"extracted_metadata": {"sha1": "0"}}]}

    while results["matching_results"] > chuck_size:
        highest_sha1 = results["results"][-1]["extracted_metadata"]["sha1"]
        results = discovery.query(environment_id,
                                  collection_id,
                                  {"count": chuck_size,
                                   "filter": "extracted_metadata.sha1.raw>" + highest_sha1,
                                   "return": "extracted_metadata.sha1",
                                   "sort": "extracted_metadata.sha1.raw"})
        set_of_sha1s |= {result["extracted_metadata"]["sha1"]
                         for result in results["results"]}

    return set_of_sha1s


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

    indexed = set_of_existing_sha1s(discovery,
                                    args.environment_id,
                                    args.collection_id)
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
