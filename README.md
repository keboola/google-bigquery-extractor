# Google BigQuery Extractor

Provide easy solution to load data from Google BigQuery to Keboola Connection.

Main functionality
-  execute query in BigQuery
-  extract query result to Cloud Storage
-  download extracted data from Cloud Storage
-  cleanup Cloud Storage of extracted data

---

# Configuration

- The `google` section
    - `projectId` - ID of the BigQuery project that will be billed for the job
    - `storage` - URI of existing Google Cloud Storage bucket, where data will be exported
- The `queries` section defines queries used to fetch data
    - `name` - query name
    - `query` - query in BigQuery Syntax (https://cloud.google.com/bigquery/query-reference)
    - `useLegacySql` *(optional)* - Use legacy SQL to run query. Set `false` to use standard SQL *(default is true)* (https://cloud.google.com/bigquery/docs/reference/standard-sql/)
    - `flattenResults` - *(optional)* Flattens all nested and repeated fields in the query results. *(default is true)*
    - `outputTable` *(optional)* - destination table ID in Keboola Connection *(if empty, will be generated automatically from query name)*
    - `primaryKey` *(optional)* - primary key in Keboola Connection
    - `incremental` *(optional)* - use incremental import to Keboola Connection *(default is false)*
    - `enabled` *(optional)* - process extraction of this query *(default is true)*

Google is slicing large results to multiple files. Primary key shoudl be defined.

## Example

```
{
  "google": [
    {
      "projectId": "some-project-123",
      "storage": "gs://some-bucket"
    }
  ],
  "queries": [
    {
      "name": "US Natality statistics",
      "query": "SELECT * FROM [dataSet.someTable] LIMIT 10",
      "primaryKey": [],
      "incremental": false,
      "flattenResults": true
    }
  ]
}
```
