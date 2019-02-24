#!/usr/bin/env python3

import singer
import requests
from datetime import datetime, timedelta
import json
import os
import csv
import pytz

from singer.utils import DATETIME_FMT_SAFE

logger = singer.get_logger().getChild('tap-rakuten')


def parse_date(string):
    return datetime.strptime(string, "%m/%d/%y")

def parse_time(string):
    return datetime.strptime(string, "%H:%M:%S").time()

def utc_datetime_string(dtime):
    return dtime.replace(tzinfo=pytz.UTC).strftime(DATETIME_FMT_SAFE)

def combine_date_time(date, time):
    return utc_datetime_string(datetime.combine(parse_date(date), parse_time(time)))

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def to_integer(string):
    try:
        return int(string)
    except ValueError:
        return None

def to_number(string):
    try:
        return float(string)
    except ValueError:
        return None

with open(get_abs_path('field_types.json')) as f:
    FIELD_TYPE_REFERENCE = json.load(f)


class APIException(Exception):
    pass


class RateLimitException(Exception):
    pass


class Rakuten():

    base_url = "https://ran-reporting.rakutenmarketing.com/{region}/reports/{report}/filters"

    default_params = {
        'include_summary': 'N',
        'tz': 'GMT',
        'date_type': 'transaction'
    }

    datetime_columns = [
        'process', 'transaction',
        'signature_match', 'transaction_created'
    ]

    def __init__(self, token, report_slug, date_type='transaction', region='en'):
        self.token = token
        self.report_slug = report_slug
        self.date_type = date_type
        self.region = region


        self._session = requests.Session()

    def get(self, date):

        logger.info("{} : requesting report CSV.".format(self.report_slug))

        day_string = date.strftime("%Y-%m-%d")

        url = self.base_url.format(region=self.region, report=self.report_slug)

        params = {
            'start_date': day_string,
            'end_date': day_string,
            'token': self.token
        }

        return self.validate_response(self._session.get(
            url,
            params={
                **self.default_params,
                **params
            },
            stream=True
        ))

    def validate_response(self, resp):
        resp.encoding = 'utf-8-sig'
        if resp.status_code in (400, 403):
            msg = resp.json()
            if msg.get('errors'):
                raise APIException("; ".join(msg.get('errors')))
            else:
                raise APIException(msg.get('message'))

        if resp.status_code == 429:
            raise RateLimitException("Too Many Requests")

        if resp.status_code in (499, 500):
            raise APIException("Server Error")

        return resp

    def get_field_data(self, columns):
        data = {}
        for name in columns:
            name = name.strip()
            field = FIELD_TYPE_REFERENCE.get(name)
            data[name] = field
        return data

    def get_column_map(self, fields):

        column_map = {}

        column_index = {f['slug']: n for n, f in fields.items()}

        slugs = column_index.keys()

        for column in self.datetime_columns:

            if all(x in slugs for x in [column + '_date', column + '_time']):
                idx = (
                    column_index[column + '_date'],
                    column_index[column + '_time']
                )

                column_map[idx] = {
                    'slug': column + '_datetime',
                    'schema': {
                        'type': ['string', 'null'],
                        'format': 'date-time'
                    },
                    'transform': lambda d, t: combine_date_time(d, t)
                }

                for id in idx:
                    del fields[id]

        for name, field in fields.items():
            schema = {}

            transform = None

            if field['type'] == 'date':
                schema['type'] = ['string', 'null']
                schema['format'] = 'date-time'
                transform = lambda d: utc_datetime_string(parse_date(d))
            elif field['type'] == 'integer':
                schema['type'] = [field['type'], 'null']
                transform = to_integer
            elif field['type'] == 'number':
                schema['type'] = [field['type'], 'null']
                transform = to_number
            else:
                schema['type'] = [field['type'], 'null']

            column_map[(name,)] = {
                'slug': field['slug'],
                'schema': schema
            }

            if transform:
                column_map[(name,)]['transform'] = transform

        return column_map

    def infer_schema(self, columns):
        field_data = self.get_field_data(columns)
        column_map = self.get_column_map(field_data)
        schema = {}
        for name, field in column_map.items():
            schema[field['slug']] = field['schema']

        return {'type': 'object', 'properties': schema}

    def transform_row(self, row, column_map=None):
        if not column_map:
            columns = row.keys()
            field_data = self.get_field_data(columns)
            column_map = self.get_column_map(field_data)

        output = {}

        for name, field in column_map.items():
            transform = field.get('transform', lambda x: x)
            output[field['slug']] = transform(*(row[n] for n in name))

        return output

    def get_schema(self):
        future_date = datetime.now() + timedelta(days=2)

        with self.get(future_date) as r:
            for line in r.iter_lines(decode_unicode=True, chunk_size=10):
                columns = line.split(",")
                break

        return self.infer_schema(columns)

    def records(self, day):

        column_map = None

        with self.get(day) as r:
            reader = csv.DictReader(
                r.iter_lines(decode_unicode=True, chunk_size=10),
                delimiter=',',
                quotechar='"'
            )
            logger.info('{} : tranforming CSV into stream.'.format(self.report_slug))
            for row in reader:
                if not column_map:
                    column_map = self.get_column_map(
                        self.get_field_data(row.keys())
                    )

                yield self.transform_row(row, column_map)
