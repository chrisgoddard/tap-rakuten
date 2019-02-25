#!/usr/bin/env python3
import pytz
import os


def to_utc(dtime):
    return dtime.replace(tzinfo=pytz.UTC)


def report_slug_to_name(slug):
    return slug.replace('-', '_').lower()


def get_abs_path(path, file=None):
    if file is None:
        file = __file__
    return os.path.join(
        os.path.dirname(os.path.realpath(file)), path)
