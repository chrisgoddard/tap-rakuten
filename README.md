# tap-rakuten

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Rakuten reporting API](https://advhelp.rakutenmarketing.com/hc/en-us/articles/206630745-Create-a-Custom-Reporting-API) _(access requires Rakuten account)_.
- Rakuten reports are configured within Rakuten and then accessible via an API endpoint.
- Outputs the schema for each report.
- Backfills based on an initial `start_date`, which can be set globally or for each individual report. Afterwards, data is pulled incrementally based on the input state.



## Configuration

Rakuten provides a customizable reporting interface where reports can be created and configured out of a list of available fields. Each report is then made available as an API endpoint, where the data can be downloaded in CSV format for a given date range.

Because the schema of the report is configured within Rakuten itself, this tap dynamically builds a schema based on the column names returned from an initial call to the API. 

**Configuration File Format**

```
{
  "region": "en",
  "token": "xxxxxxx",
  "date_type": "transaction",
  "start_date": "2019-01-01T00:00:00Z",
  "report_slug": "report-slug"
}

```

Additionally, the region should be set to how it appears in this URL - though 

```
https://ran-reporting.rakutenmarketing.com/{region}/reports/{report_slug}/filters?date_range=...
```

### Discovery mode

This command returns a JSON that describes the schema of each table.

```
$ tap-rakuten --config config.json --discover
```

To save this to `catalog.json`:

```
$ tap-rakuten --config config.json --discover > catalog.json
```


### Sync Mode

With an annotated `catalog.json`, the tap can be invoked in sync mode:

```
$ tap-rakuten --config config.json --catalog catalog.json
```

Messages are written to standard output following the Singer specification. The resultant stream of JSON data can be consumed by a Singer target.


## Replication Methods and State File

Use the following command to pipe tap into your Singer target of choice and update the state file in one go.

```
tap-rakuten --config config.json --catalog catalog.json --state state.json | target > state.json.tmp && tail -1 state.json.tmp > state.json
```
