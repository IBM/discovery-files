#!/usr/bin/env python3

import argparse
import json
import os
import glob
import queue
import time
from base64 import urlsafe_b64encode
from hashlib import sha384


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
        self.queue = queue.Queue()
        self.start_time = time.perf_counter()
        self.wait_until = self.start_time

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

    def push_work(self, item):
        """Push an item into our work queue."""
        self.queue.put(item)

    def finish(self):
        """Block until all tasks are done then return runtime."""
        self.queue.join()
        time_pushing = time.perf_counter() - self.start_time
        # Tell my worker threads to finish
        for _ in range(self.thread_count):
            self.queue.put(None)
        return time_pushing


def writable_environment_id(discovery):
    for environment in discovery.get_environments()["environments"]:
        if not environment["read_only"]:
            return environment["environment_id"]


def set_of_indexed_filenames(creds):
    discovery = DiscoveryV1(creds.get("version", "2017-08-01"),
                            url=creds["url"],
                            username=creds["username"],
                            password=creds["password"])

    results = discovery.query(creds["environment_id"],
                              creds["collection_id"],
                              {"return": "extracted_metadata.filename"})

    return {result["extracted_metadata"]["filename"].split("-")[1]
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

    print(args)

    path = args["Path_directory"]
    work = Args(args)

    try:
        indexed = set_of_indexed_filenames(args)
    except:
        indexed = set()

    for infile in glob.glob(os.path.join(path, '*.fasta')):

        if indexed:
            print("Ingesting", infile,
                  "skipping", len(indexed), "items found in the index")
        else:
            print("Ingesting", infile)

        print("current file is: " + infile)

        in_count = 0
        push_count = 0
        if infile["file_name"] not in indexed:
            work.push_work((infile["file_name"],
                            hash_url(infile["url"]),
                            json.dumps(infile)))
            push_count += 1
        in_count += 1

    time_pushing = work.finish()
    print("Completed pushing", push_count, "documents in",
          time_pushing, "seconds")


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


@staticmethod
def hash_url(url):
    """Mash a URL into a form that can be used as a document_id
       in Watson Discovery."""
    return urlsafe_b64encode(sha384(bytes(url, "UTF-8")).digest()) \
        .decode("UTF-8")


if __name__ == "__main__":
    main(parse_command_line())
