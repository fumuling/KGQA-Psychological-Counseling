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


from __future__ import division

from datetime import timedelta
from random import uniform
from time import sleep

from monotonic import monotonic


class Timer(object):

    @classmethod
    def repeat(cls, at_least, timeout, snooze=0.0, snooze_multiplier=1.0, snooze_jitter=0.0):
        """ Yield an incrementing timer at least `at_least` times,
        thereafter continuing until the timeout has been reached.
        """
        timer = cls(timeout)
        n = 0
        next_snooze = None
        while n < at_least or timer.remaining():
            if snooze:
                if next_snooze is None:
                    next_snooze = snooze
                else:
                    delay = next_snooze * uniform(1 - (snooze_jitter / 2), 1 + (snooze_jitter / 2))
                    sleep(delay if delay > 0 else 0)
                    next_snooze *= snooze_multiplier
            yield timer
            n += 1

    def __init__(self, seconds):
        self.__t0 = t0 = monotonic()
        self.__t1 = t0 + (seconds or 0)

    def __bool__(self):
        return self.remaining() > 0

    __nonzero__ = __bool__

    def __repr__(self):
        t = monotonic()
        return "<Timer at %.09fs>" % (t - self.__t0)

    def passed(self):
        return monotonic() - self.__t0

    def remaining(self):
        diff = self.__t1 - monotonic()
        return diff if diff > 0 else 0.0


def millis_to_timedelta(t):
    if t is None:
        return None
    else:
        return timedelta(milliseconds=t)
