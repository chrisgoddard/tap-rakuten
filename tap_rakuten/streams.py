import singer
from singer import metadata
from singer import utils
from datetime import datetime, timedelta
import pytz


def to_utc(dtime):
    return dtime.replace(tzinfo=pytz.UTC)


def report_slug_to_name(slug):
    return slug.replace('-', '_').lower()


class Stream():
    replication_method = 'INCREMENTAL'

    start_date = None

    schema = None

    stream = None

    def __init__(self, client, stream_config):
        self.name = stream_config.get('report_slug')
        self.tap_stream_id = report_slug_to_name(self.name)
        self.client = client
        self.utcnow = datetime.utcnow().replace(tzinfo=pytz.UTC)
        self.start_date = stream_config.get('start_date')
        self.date_type = stream_config.get('date_type')

    def load_schema(self):
        self.set_schema(self.client.get_schema(self.name))

    def set_schema(self, schema):
        self.schema = schema

    def iterdates(self, start_date):
        # set to start of day
        date = to_utc(
            datetime.combine(
                start_date.date(),
                datetime.min.time()
            )
        )

        for n in range(int((self.utcnow - date).days)):
            yield date + timedelta(n)

    def get_metadata(self):
        keys = self.schema.get('properties').keys()

        self.key_properties = [k for k in keys if 'date' in k]

        mdata = metadata.new()

        mdata = metadata.write(
            mdata,
            (),
            'table-key-properties',
            self.key_properties
        )

        mdata = metadata.write(
            mdata,
            (),
            'forced-replication-method',
            'INCREMENTAL'
        )

        for field_name in keys:
            if field_name in self.key_properties:
                mdata = metadata.write(
                    mdata,
                    ('properties', field_name),
                    'inclusion',
                    'automatic'
                )
            else:
                mdata = metadata.write(
                    mdata,
                    ('properties', field_name),
                    'inclusion',
                    'available'
                )

            mdata = metadata.write(
                mdata,
                ('properties', field_name),
                'selected-by-default',
                True
            )

        return metadata.to_list(mdata)

    def get_bookmark(self, state):
        return singer.get_bookmark(state, self.name, "last_sync")

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        if not bookmark:
            bookmark = self.start_date

        start = utils.strptime_with_tz(bookmark)

        for start_date in self.iterdates(start):

            for item in self.client.report(
                self.name,
                start_date=start_date,
                date_type=self.date_type
            ):
                yield (self.stream, item)

            singer.write_bookmark(
                state,
                self.tap_stream_id,
                "last_sync",
                utils.strftime(start_date)
            )
            singer.write_state(state)


def get_stream(client, config, report):
    defaults = {
        'start_date': config.get('default_start_date'),
        'date_type': config.get('default_date_type')
    }
    return Stream(client, {**defaults, **report})


def get_streams(client, config):
    for report in config.get('reports', []):
        yield get_stream(client, config, report)
