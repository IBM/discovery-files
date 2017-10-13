# disco-files
An extremely simple tool to send files into Watson Discovery, with automatic error handling and simple retry and maybe refresh.

![Book cover of "The Disco Files"](discofilescover.jpg)

## Team members

- [Becca Makar](https://github.ibm.com/Rebecca-Makar)
- [Bruce Adams](https://github.ibm.com/ba)
- [Phil Anderson](https://github.ibm.com/Phil-Anderson)

## Command line

```
./discofiles.py -h
usage: discofiles.py [-h] [-credentials CREDENTIALS] path [path ...]

Send files into Watson Discovery

positional arguments:
  path                  File or directory of files to send to Discovery

optional arguments:
  -h, --help            show this help message and exit
  -credentials CREDENTIALS
                        JSON file containing Discovery service credentials;
                        default: "credentials.json"
```
