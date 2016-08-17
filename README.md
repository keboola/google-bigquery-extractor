# Google BigQuery Extractor

Provide easy solution to load data from Google BigQuery to Keboola Connection.

Main functionality
-  execute query in BigQuery
-  extract query result to Cloud Storage
-  download extracted data from Cloud Storage
-  cleanup Cloud Storage of extracted data

---

# Configuration

- The `queries` section defines queries used to fetch data
    - `name` - query name
    - `query` - query in BigQuery Syntax (https://cloud.google.com/bigquery/query-reference)
    - `projectId` - ID of the BigQuery project that will be billed for the job
    - `flattenResults` - *(optional)* Flattens all nested and repeated fields in the query results. *(default is true)*
    - `storage` - URI of existing Google Cloud Storage bucket, where data will be exported
    - `outputTable` *(optional)* - destination table ID in Keboola Connection *(if empty, will be generated automatically from query name)*
    - `primaryKey` *(optional)* - primary key in Keboola Connection
    - `incremental` *(optional)* - use incremental import to Keboola Connection *(default is false)*

Google is slicing large results to multiple files. Primary key shoudl be defined.

## Example

```
{
  "queries": [
    {
      "name": "US Natality statistics",
      "query": "SELECT * FROM [dataSet.someTable] LIMIT 10",
      "storage": "gs://some-bucket",
      "primaryKey": [],
      "incremental": false,
      "projectId": "some-project-123",
      "flattenResults": true
    }
  ]
}
```
