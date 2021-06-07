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


from os import path
from os.path import dirname

from six.moves import input

from py2neo.vendor.bottle import (
    get,
    post,
    redirect,
    request,
    static_file,
    template,
    TEMPLATE_PATH,
)

from py2neo import ConnectionProfile
from py2neo.ogm import Repository
from py2neo.ogm.models.movies import Movie, Person


HOME = dirname(__file__)
STATIC = path.join(HOME, "static")
VIEWS = path.join(HOME, "views")
CONFIG = path.join(HOME, "movies.ini")

# Update the template search path used by Bottle
TEMPLATE_PATH.append(VIEWS)

# Load the connection details from the config file
profile = ConnectionProfile.from_file(CONFIG, "Neo4j")

# Set up a link to the local graph database.
repo = Repository(profile)

if repo.match(Movie).count() == 0:
    print("No movie data found in repository %r" % repo)
    answer = input("Would you like to automatically load the Neo4j movies data set [y/N]? ")
    if answer and answer[0].upper() == "Y":
        with open(path.join(HOME, "data", "movies.cypher")) as fin:
            query = fin.read()
        repo.graph.run(query)
        print("Movie data loaded - %d movies available" % repo.match(Movie).count())
    else:
        exit(1)


@get("/static/<filename>")
def get_static(filename):
    """ Static file accessor.
    """
    return static_file(filename, root=STATIC)


@get("/")
def get_index():
    """ Index page.
    """
    return template("index")


@get("/person/")
def get_person_list():
    """ List of all people.
    """
    return template("person_list", people=repo.match(Person).order_by("_.name"))


@get("/person/<name>")
def get_person(name):
    """ Page with details for a specific person.
    """
    person = repo.get(Person, name)
    movies = [(movie.title, "Actor") for movie in person.acted_in] + \
             [(movie.title, "Director") for movie in person.directed]
    return template("person", person=person, movies=movies)


@get("/movie/")
def get_movie_list():
    """ List of all movies.
    """
    return template("movie_list", movies=repo.match(Movie).order_by("_.title"))


@get("/movie/<title>")
def get_movie(title):
    """ Page with details for a specific movie.
    """
    return template("movie", movie=repo.get(Movie, title))


@post("/movie/review")
def post_movie_review():
    """ Capture review and redirect back to movie page.
    """
    movie = repo.get(Movie, request.forms["title"])
    movie.reviewers.add(Person(request.forms["name"]),
                        summary=request.forms["summary"],
                        rating=request.forms["rating"])
    repo.save(movie)
    return redirect("/movie/%s" % movie.title)
