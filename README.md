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
  "default_date_type": "transaction",
  "default_start_date": "2019-01-01T00:00:00Z",
  "reports": [
      {
          "report_slug": "report-slug",
      },
      {
          "report_slug": "report-slug",
          "date_type": "process",
          "start_date": "2019-02-01T00:00:00Z"
      }
  ]
}

```

Multiple reports can be synced by defining them as objects in the "reports" array. The only required field within an report is the "report_slug", which is the name of the report as it appears in the "Get API" pop up.

Additionally, the region should be set to how it appears in this URL - though 

```
https://ran-reporting.rakutenmarketing.com/{region}/reports/{report_slug}/filters?date_range=...
```

Additionally, the region is