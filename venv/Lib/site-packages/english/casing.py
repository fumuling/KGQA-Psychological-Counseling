#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2020, Nigel Small
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


from functools import reduce
from operator import add
from re import findall


def iter_words(s):
    if " " in s:
        for word in s.split():
            yield word
    elif "_" in s:
        for word in s.split("_"):
            yield word
    elif "-" in s:
        for word in s.split("-"):
            yield word
    elif s.isupper():
        yield s
    else:
        for word in findall(r"[A-Z]?[^A-Z]*", s):
            if word:
                yield word


class Words(object):

    def __init__(self, words):
        if isinstance(words, tuple):
            words = reduce(add, map(tuple, map(iter_words, words)), ())
        else:
            words = iter_words(words)
        self.words = tuple(word for word in words if word)

    def upper(self, separator=" "):
        return separator.join(word.upper() for word in self.words)

    def lower(self, separator=" "):
        return separator.join(word.lower() for word in self.words)

    def title(self):
        all_caps = all(word.isupper() for word in self.words)

        def title_word(word):
            if not word or (word.isupper() and not all_caps):
                return word
            else:
                return word[0].upper() + word[1:]

        return " ".join(map(title_word, self.words))

    def snake(self):
        return self.lower("_")

    def camel(self, upper_first=False):
        s = "".join(word[0].upper() + word[1:].lower() for word in self.words)
        if upper_first:
            return s
        else:
            return s[0].lower() + s[1:]
