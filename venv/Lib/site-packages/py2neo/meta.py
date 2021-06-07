#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2021, Nigel Small
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from os import getenv, path


PACKAGE_NAME = "py2neo"
PACKAGE_DESCRIPTION = "Python client library and toolkit for Neo4j"

VERSION_FILE = path.join(path.dirname(__file__), "VERSION")


def _parse_letter_version(letter, number):

    if letter:
        # We consider there to be an implicit 0 in a pre-release if there is
        # not a numeral associated with it.
        if number is None:
            number = 0

        # We normalize any letters to their lower case form
        letter = letter.lower()

        # We consider some words to be alternate spellings of other words and
        # in those cases we want to normalize the spellings to our preferred
        # spelling.
        if letter == "alpha":
            letter = "a"
        elif letter == "beta":
            letter = "b"
        elif letter in ["c", "pre", "preview"]:
            letter = "rc"
        elif letter in ["rev", "r"]:
            letter = "post"

        return letter, int(number)
    if not letter and number:
        # We assume if we are given a number, but we are not given a letter
        # then this is using the implicit post release syntax (e.g. 1.0-1)
        letter = "post"

        return letter, int(number)

    return None


def parse_version_string(version_string):
    import re

    version_pattern_str = r"""
        v?
        (?:
            (?:(?P<epoch>[0-9]+)!)?                           # epoch
            (?P<release>[0-9]+(?:\.[0-9]+)*)                  # release segment
            (?P<pre>                                          # pre-release
                [-_\.]?
                (?P<pre_l>(a|b|c|rc|alpha|beta|pre|preview))
                [-_\.]?
                (?P<pre_n>[0-9]+)?
            )?
            (?P<post>                                         # post release
                (?:-(?P<post_n1>[0-9]+))
                |
                (?:
                    [-_\.]?
                    (?P<post_l>post|rev|r)
                    [-_\.]?
                    (?P<post_n2>[0-9]+)?
                )
            )?
            (?P<dev>                                          # dev release
                [-_\.]?
                (?P<dev_l>dev)
                [-_\.]?
                (?P<dev_n>[0-9]+)?
            )?
        )
    """

    version_pattern = re.compile(
        r"^\s*" + version_pattern_str + r"\s*$",
        re.VERBOSE | re.IGNORECASE,
    )

    match = version_pattern.search(version_string)
    if not match:
        raise ValueError("Invalid version: {}".format(version_string))

    return {
        "string": version_string,
        "epoch": int(match.group("epoch")) if match.group("epoch") else 0,
        "release": tuple(int(i) for i in match.group("release").split(".")),
        "pre": _parse_letter_version(match.group("pre_l"), match.group("pre_n")),
        "post": _parse_letter_version(
            match.group("post_l"), match.group("post_n1") or match.group("post_n2")
        ),
        "dev": _parse_letter_version(match.group("dev_l"), match.group("dev_n")),
    }


def get_version_data():
    rtd_version = getenv("READTHEDOCS_PROJECT") == PACKAGE_NAME and getenv("READTHEDOCS_VERSION")
    if rtd_version and rtd_version not in ("latest", "stable"):
        version_string = rtd_version
    else:
        with open(VERSION_FILE) as f:
            version_string = f.read().strip()
    data = parse_version_string(version_string)
    data["rtd"] = rtd_version
    return data


def get_metadata():

    version_data = get_version_data()
    source_url = "https://github.com/technige/py2neo"
    release = version_data["release"]

    return {
        "name": PACKAGE_NAME,
        "version": (version_data["string"]),
        "description": PACKAGE_DESCRIPTION,
        "author": "Nigel Small",
        "author_email": "py2neo@nige.tech",
        "url": "https://py2neo.org/",
        "project_urls": {
            "Bug Tracker": "{}/issues".format(source_url),
            "Documentation": "https://py2neo.org/{}.{}/".format(release[0], release[1]),
            "Source Code": source_url,
        },
        "license": "Apache License, Version 2.0",
        "keywords": [],
        "platforms": [],
        "classifiers": [
            "Development Status :: 6 - Mature",
            "Environment :: Console",
            "Intended Audience :: Developers",
            "Intended Audience :: Information Technology",
            "Intended Audience :: Science/Research",
            "License :: OSI Approved :: Apache Software License",
            "Natural Language :: English",
            "Operating System :: OS Independent",
            "Programming Language :: Python",
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.4",
            "Programming Language :: Python :: 3.5",
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: Implementation :: CPython",
            "Topic :: Database",
            "Topic :: Database :: Database Engines/Servers",
            "Topic :: Scientific/Engineering",
            "Topic :: Software Development",
            "Topic :: Software Development :: Libraries",
        ],
    }
