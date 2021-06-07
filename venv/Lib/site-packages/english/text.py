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


def first_sentence(text):
    """ Extract the first sentence of a text.
    """
    if not text:
        return ""

    from re import match
    lines = text.splitlines(False)
    one_line = " ".join(lines)
    matched = match(r"^(.*?(?<!\b\w)[.?!])\s+[A-Z0-9]", one_line)
    if matched:
        return matched.group(1)
    else:
        return lines[0]
