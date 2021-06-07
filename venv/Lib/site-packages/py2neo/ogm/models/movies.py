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


"""
This module contains OGM models suitable for use with the Neo4j movies
database.
"""

from py2neo.ogm import Model, Property, RelatedTo, RelatedFrom


class Movie(Model):
    __primarykey__ = "title"

    title = Property()
    tagline = Property()
    released = Property()

    actors = RelatedFrom("Person", "ACTED_IN")
    directors = RelatedFrom("Person", "DIRECTED")
    producers = RelatedFrom("Person", "PRODUCED")
    writers = RelatedFrom("Person", "WROTE")
    reviewers = RelatedFrom("Person", "REVIEWED")

    def __lt__(self, other):
        return self.title < other.title


class Person(Model):
    __primarykey__ = "name"

    name = Property()
    born = Property()

    acted_in = RelatedTo(Movie)
    directed = RelatedTo(Movie)
    produced = RelatedTo(Movie)
    wrote = RelatedTo(Movie)
    reviewed = RelatedTo(Movie)

    def __init__(self, name=None):
        self.name = name

    def __lt__(self, other):
        return self.name < other.name
