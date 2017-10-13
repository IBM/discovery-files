#!/usr/bin/env python3

import argparse
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

            try:
                with open(item, "rb") as f:
                    self.discovery.add_document(self.environment_id,
                                                self.collection_id,
                                                f)
            except:
                exception = sys.exc_info()[1]
                exception_code_string = exception.args[0]
                parsed_string = exception_code_string[-3:]
                if parsed_string == "429":
                    self.wait_until = time.perf_counter() + 5
                    self.queue.put(item)
                else:
                    print("Failing because it is", exception_code_string)
                    self.counts[parsed_string] = self.counts.get(
                        parsed_string, 0) + 1
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


def set_of_indexed_filenames(discovery,
                             environment_id,
                             collection_id):
    results = discovery.query(environment_id,
                              collection_id,
                              {"count": 10000,
                               "return": "extracted_metadata.filename"})
    return {result["extracted_metadata"]["filename"]
            for result in results["results"]}


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

    indexed = set_of_indexed_filenames(discovery,
                                       args.environment_id,
                                       args.collection_id)
    count_ignore = 0
    count_ingest = 0
    for path in args.paths:
        for root, _dirs, files in os.walk(path):
            for name in files:

                this_path = os.path.join(root, name)
                if name in indexed:
                    count_ignore += 1
                else:
                    count_ingest += 1
                    work.put_in_queue(this_path)

    print("Ignored", count_ignore, "file(s), because they were found in collection.",
          "\nIngesting", count_ingest, "file(s).")

    work.finish()


def parse_command_line():
    parser = argparse.ArgumentParser(
        description="Send files into Watson Discovery")
    parser.add_argument("path",
                        nargs="+",
                        help="File or directory of files to send to Discovery")
    parser.add_argument("-credentials",
                        default="credentials.json",
                        help='JSON file containing Discovery service credentials; default: "credentials.json"')
    parsed = parser.parse_args()
    with open(parsed.credentials) as creds_file:
        args = Args(json.load(creds_file))
    args.paths = parsed.path
    return args


if __name__ == "__main__":
    main(parse_command_line())
