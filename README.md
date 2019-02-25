# tap-rakuten

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the Rakuten reporting API.
- Rakuten reports are configured within Rakuten and then accessible via an API endpoint.
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---
