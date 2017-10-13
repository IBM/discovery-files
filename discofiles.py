#!/usr/bin/env python3

import argparse
import json
import os

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
    try:
        indexed = set_of_indexed_filenames(args)
    except:
        indexed = set()
    for path in args.paths:
        for root, dirs, files in os.walk(path):
            for name in files:

                this_path = os.path.join(root, name)
                print(this_path)
                if name in indexed:
                    print("Ignoring this", name, "because it is already there")
                else:
                    print("Ingesting", name)
                    discovery.add_document(args.environment_id,
                                           args.collection_id,
                                           this_path)


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
