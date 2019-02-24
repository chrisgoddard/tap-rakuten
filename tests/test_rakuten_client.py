#!/usr/bin/env python3

import unittest
from pprint import pprint
from tap_rakuten.client import Rakuten

test_columns = [
    "# of Clicks",
    "Sales",
    "Publisher ID",
    "Publisher Name",
    "Transaction Date",
    "Transaction Time",
    "Transaction Created On Time",
    "Signature Match Date"
]

test_fields = {
    "# of Clicks": {
        "slug": "num_of_clicks",
        "type": "integer"
    },
    "Sales": {
        "slug": "sales",
        "type": "number"
    },
    "Publisher ID": {
        "slug": "publisher_id",
        "type": "integer"
    },
    "Publisher Name": {
        "slug": "publisher_name",
        "type": "string"
    },
    "Transaction Date": {
        "slug": "transaction_date",
        "type": "date"
    },
    "Transaction Time": {
        "slug": "transaction_time",
        "type": "string"
    },
    "Transaction Created On Time": {
        "slug": "transaction_created_on_time",
        "type": "string"
    },
    "Signature Match Date": {
        "slug": "signature_match_date",
        "type": "date"
    }
}

test_row = {
    "# of Clicks": 5,
    "Sales": 35.5,
    "Publisher ID": 1000001,
    "Publisher Name": "Test Publisher",
    "Transaction Date": "2/22/19",
    "Transaction Time": "10:00:05",
    "Transaction Created On Time": "10:00:01",
    "Signature Match Date": "12/12/18"
}

test_transformed_row = {
    "num_of_clicks": 5,
    "sales": 35.5,
    "publisher_id": 1000001,
    "publisher_name": "Test Publisher",
    "transaction_datetime": "2019-02-22T10:00:05.000000Z",
    "transaction_created_on_time": "10:00:01",
    "signature_match_date": "2018-12-12T00:00:00.000000Z"
}

test_schema = {
    "type": "object",
    "properties": {
        "num_of_clicks": {
            "type": ["integer", "null"]
        },
        "sales": {
            "type": ["number", "null"]
        },
        "publisher_id": {
            "type": ["integer", "null"]
        },
        "publisher_name": {
            "type": ["string", "null"]
        },
        "transaction_datetime": {
            "type": ["string", "null"],
            "format": "date-time"
        },
        "transaction_created_on_time": {
            "type": ["string", "null"]
        },
        "signature_match_date": {
            "type": ["string", "null"],
            "format": "date-time"
        }
    }
}

class Test_RakutenClient(unittest.TestCase):

    def test_get_field_data(self):

        rak = Rakuten("TOKEN", "slug")

        results = rak.get_field_data(test_columns)

        for name, field in results.items():
            self.assertDictEqual(field, test_fields[name])

    def test_infer_schema(self):

        self.maxDiff = None

        rak = Rakuten("TOKEN", "slug")

        schema = rak.infer_schema(test_columns)

        self.assertDictEqual(
            schema,
            test_schema
        )

    def test_transform_row(self):

        rak = Rakuten("TOKEN", "slug")

        row = rak.transform_row(test_row)

        self.assertDictEqual(
            row,
            test_transformed_row
        )

    # def test_get_schema(self):
    #     pass



if __name__ == '__main__':
    unittest.main()