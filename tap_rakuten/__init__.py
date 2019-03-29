#!/usr/bin/env python3
import os
import json
import singer

from singer import utils, metadata
from tap_rakuten.client import Rakuten
from tap_rakuten.streams import get_stream
from tap_rakuten.sync import sync_stream

REQUIRED_CONFIG_KEYS = [
    "token", "region", "report_slug",
    "start_date", "date_type"
]

logger = singer.get_logger().getChild('tap-rakuten')


def discover(client, config):
    """
    Discover availalbe streams from provided configuration.
    Schema is generated dynamically based on returned column names.
    """

    # modified from supporting multiple reports to just one report at a time

    streams = []

    stream = get_stream(client, config)

    stream.load_schema()

    catalog_entry = {
        'stream': stream.name,
        'tap_stream_id': stream.tap_stream_id,
        'schema': stream.schema,
        'metadata': stream.get_metadata(),
    }

    streams.append(catalog_entry)

    return {'streams': streams}


def get_selected_streams(catalog):
    """
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    """
    selected_streams = []
    for stream in catalog.streams:
        stream_metadata = metadata.to_map(stream.metadata)
        # stream metadata will have an empty breadcrumb
        if metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream.tap_stream_id)

    return selected_streams


def sync(client, catalog, state, config):
    """
    Sync streams.
    """

    selected_stream_ids = get_selected_streams(catalog)

    reports = {}
    for report in config.get('reports', []):
        reports[report.get('report_slug')] = report

    for stream in catalog.streams:

        stream_id = stream.tap_stream_id

        mdata = metadata.to_map(stream.metadata)

        if stream_id not in selected_stream_ids:
            logger.info("%s: Skipping - not selected", stream_id)
            continue

        singer.write_schema(
            stream_id,
            stream.schema.to_dict(),
            metadata.get(mdata, (), 'table-key-properties')
        )

        instance = get_stream(
            client,
            config
        )

        instance.stream = stream

        counter_value = sync_stream(state, instance)

        logger.info(
            "%s: Completed sync (%s rows)",
            stream.tap_stream_id,
            counter_value
        )


@utils.handle_top_exception(logger)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    client = Rakuten(
        token=args.config['token'],
        region=args.config['region'],
        date_type=args.config['date_type']
    )

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover(client, args.config)
        print(json.dumps(catalog, indent=2))

    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover(client, args.config)

        sync(
            client,
            catalog,
            args.state,
            args.config
        )


if __name__ == "__main__":
    main()
