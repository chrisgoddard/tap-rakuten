#!/usr/bin/env python3
import singer
import requests
import json
import os
import csv
import pytz

from datetime import datetime, timedelta
from singer.utils import DATETIME_FMT_SAFE

logger = singer.get_logger().getChild('tap-rakuten')


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


with open(get_abs_path('field_types.json')) as f:
    FIELD_TYPE_REFERENCE = json.load(f)


def parse_date(string):
    return datetime.strptime(string, "%m/%d/%y")


def parse_time(string):
    return datetime.strptime(string, "%H:%M:%S").time()


def utc_datetime_string(dtime):
    return dtime.replace(tzinfo=pytz.UTC).strftime(DATETIME_FMT_SAFE)


def combine_date_time(date, time):
    return utc_datetime_string(
        datetime.combine(parse_date(date), parse_time(time))
    )


def to_clean_string(string):
    if string == 'null':
        return None
    elif string == '':
        return None
    return string


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


def to_datetime(string):
    try:
        return utc_datetime_string(parse_date(string))
    except ValueError:
        return None


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
        'process',
        'transaction',
        'signature_match',
        'transaction_created'
    ]

    def __init__(self, token, region='en', date_type='transaction'):
        self.token = token
        self.region = region

        if date_type in ('transaction', 'process'):
            self.default_params['date_type'] = date_type

        self._session = requests.Session()

    def get_params(self, **kwargs):
        """
        Merge arguments with default request parameters. Ensures only allowed
        parameters are passed through to request and transform datetime objects
        into correct format. `include_summary` is always set to N as that would
        break CSV parsing.

        Args:
            start_date (datetime.datetime): start date of the report
            end_date (datetime.datetime, optional): will be set to start_date
                if not set
            tz (string, optional): valid timezone name
            date_type (string, optional): must be either `transaction`
                or `process`

        Returns:
            params (dict)

        """

        # must include start_date
        assert 'start_date' in kwargs.keys()

        allowed_keys = [
            'start_date',
            'end_date',
            'date_type'
        ]

        # ensure only allowed arguments
        kwargs = {k: v for k, v in kwargs.items() if k in allowed_keys}

        params = {
            'token': self.token
        }

        for key, value in kwargs.items():
            if isinstance(value, datetime):
                value = value.strftime("%Y-%m-%d")

            if key == 'transaction':
                if value not in ('transaction', 'process'):
                    value = None

            if value:
                params[key] = value

        keys = params.keys()

        if ('start_date' in keys) and ('end_date' not in keys):
            params['end_date'] = params['start_date']

        return {
            **self.default_params,
            **params
        }

    def get(self, report_slug, **kwargs):
        """
        Request CSV report from Rakuten.

        Arguments:
            report_slug (string): name of report
            start_date (datetime.datetime): start day of report
            end_date (datetime.datetime, optional): end day of report
            date_type (string, optional): must be either `transaction`
                or `process`

        Returns:
            response (requests.Response)

        """

        url = self.base_url.format(region=self.region, report=report_slug)

        params = self.get_params(**kwargs)

        return self.validate_response(self._session.get(
            url,
            params=params,
            stream=True
        ))

    def validate_response(self, resp):
        """
        Set appropriate text encoding on response and handle and errors.

        Arguments:
            resp (requests.Response)

        Returns:
            resp (requests.Response)
        """
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
        """
        Get field type data for columns listed

        Arguments:
            columns (list): list of column names from CSV response

        Returns:
            field_data (dict)
        """
        data = {}
        for name in columns:
            name = name.strip()
            field = FIELD_TYPE_REFERENCE.get(name)
            data[name] = field
        return data

    def get_column_map(self, fields):
        """
        Creates a "column map" dictionary, with tuple keys of CSV column names
        containing corresponding slug (what be the eventual field name in the
        returned data), the field's schema definition and a tranform function
        that accepts the columns values. Multiple column names can be mapped to
        a single field, i.e. (ColumnA, ColumnB), and the transform function would
        then accept two arguments.

        This is to allow date and time fields to be concatenated into a single
        datetime field.

        Args:
            fields (dict): field data as provided by get_field_data method

        Returns:
            column_name: (dict): (column, [column]): {'slug':str, 'schema': {},
                'tranform': function}

        """

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
                    'transform': combine_date_time
                }

                for id in idx:
                    del fields[id]

        for name, field in fields.items():
            schema = {}
            transform = None
            if field['type'] == 'date':
                schema['type'] = ['string', 'null']
                schema['format'] = 'date-time'
                transform = to_datetime
            elif field['type'] == 'integer':
                schema['type'] = [field['type'], 'null']
                transform = to_integer
            elif field['type'] == 'number':
                schema['type'] = [field['type'], 'null']
                transform = to_number
            elif field['type'] == 'string':
                schema['type'] = [field['type'], 'null']
                transform = to_clean_string

            column_map[(name,)] = {
                'slug': field['slug'],
                'schema': schema
            }

            if transform:
                column_map[(name,)]['transform'] = transform

        return column_map

    def infer_schema(self, columns):
        """
        Infer the schema from a set of column names.

        Args:
            columns (list): list of raw CSV column names

        Returns:
            schema (dict): valid schema definition
        """
        field_data = self.get_field_data(columns)
        column_map = self.get_column_map(field_data)
        schema = {}
        for name, field in column_map.items():
            schema[field['slug']] = field['schema']

        return {'type': 'object', 'properties': schema}

    def transform_row(self, row, column_map=None):
        """
        Transform a row from the CSV DictReader into an output row
        where the names and types have been standardized based on a provided
        column_map (from get_column_map method).

        If column_map is not provided it will be generated based on the keys of
        of the current row. However in iteration it is faster to define this
        once and provide in subsequent calls.

        Args:
            row (dict): a row from CSV DictReader
            column_map (dict, optional): column man from get_column_map method

        Returns:
            row (dict): transformed output row
        """
        if not column_map:
            columns = row.keys()
            field_data = self.get_field_data(columns)
            column_map = self.get_column_map(field_data)

        output = {}

        for name, field in column_map.items():
            transform = field.get('transform', lambda x: x)
            output[field['slug']] = transform(*(row[n] for n in name))

        return output

    def get_schema(self, report_slug):
        """
        Get the schema of a report from a report_slug.

        This method requests a report from a future date which will return a
        CSV with headers but no rows. This means faster download time for
        initial schema definition.

        Args:
            report_slug (string): valid report slug

        Returns:
            schema (dict): valid schema definition
        """

        logger.info("{} : determining schema.".format(report_slug))

        future_date = datetime.now() + timedelta(days=2)

        with self.get(report_slug, start_date=future_date) as r:
            for line in r.iter_lines(decode_unicode=True, chunk_size=10):
                columns = line.split(",")
                break

        return self.infer_schema(columns)

    def report(self, report_slug, start_date, **kwargs):
        """
        Generate a report for a particular report_slug and start_date.

        Only start_date required for a single day period. end_date will be
        automatically set to the same day.

        Args:
            report_slug (string): slug of request report
            start_date (datetime.datetime): start date of report
            end_date (datetime.datetime, optional): end day of report
            date_type (string, optional): must be either `transaction`
                or `process`

        Yields:
            row (dict): a single standardized row from the report
        """
        column_map = None

        logger.info("{} : requesting {:%Y-%m-%d} report CSV.".format(
            report_slug, start_date
        ))

        with self.get(report_slug, start_date=start_date, **kwargs) as r:
            reader = csv.DictReader(
                r.iter_lines(decode_unicode=True),
                delimiter=',',
                quotechar='"'
            )
            logger.info('{} : processing CSV data.'.format(
                report_slug
            ))
            for row in reader:
                if not column_map:
                    column_map = self.get_column_map(
                        self.get_field_data(row.keys())
                    )

                yield self.transform_row(row, column_map)
