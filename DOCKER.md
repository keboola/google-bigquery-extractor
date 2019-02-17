## Docker Development

Clone this repository and init the workspace with following command:

```
git clone git@github.com:keboola/google-bigquery-extractor.git
cd google-bigquery-extractor

```

#### 1. Prepare Google Cloud Application
Go to the Google Cloud Console https://console.cloud.google.com/ and create a new project for your application.

In **`Navigation menu`** select **`APIs & Services > Credentials`** and create new _OAuth client ID_ credentials.
- Choose `Web application` as _Application type_
- Fill `https://developers.google.com/oauthplayground` in _Authorized redirect URIs_

Store the created OAuth client credentials into `.env` file
```
BIGQUERY_EXTRACTOR_APP_KEY={OAuth Client ID}
BIGQUERY_EXTRACTOR_APP_SECRET={OAuth Client secret}
```

#### 2. Authorize API

Use [OAuth 2.0 Playground](https://developers.google.com/oauthplayground/#step1&scopes=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fbigquery%20https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fdevstorage.read_write&url=https%3A//&content_type=application/json&http_method=GET&useDefaultOauthCred=checked&oauthEndpointSelect=Google&oauthAuthEndpointValue=https%3A//accounts.google.com/o/oauth2/auth&oauthTokenEndpointValue=https%3A//www.googleapis.com/oauth2/v3/token&includeCredentials=unchecked&accessTokenType=bearer&autoRefreshToken=unchecked&accessType=offline&forceAprovalPrompt=checked&response_type=code) to authorize API and retrieve access tokens for your account.
- Select _Scopes_ `https://www.googleapis.com/auth/bigquery` and `https://www.googleapis.com/auth/devstorage.read_write` required by extractor
- Check _Use your own OAuth credentials_ and fill credentials from the first step.

Click on the **`Authorize APIs`** button and authorize application to access your Google BigQuery and Google Cloud Storage data.

Finally click on the **`Exchange authorization code for tokens`** button which gives you access and refresh tokens in JSON format.

```json
{
  "access_token": "XXXXXXXXX", 
  "scope": "https://www.googleapis.com/auth/devstorage.read_write https://www.googleapis.com/auth/bigquery", 
  "token_type": "Bearer", 
  "expires_in": 3600, 
  "refresh_token": "YYYYYYYYY"
}
```

Store this credentials JSON into `.env` file as single-line json_encoded string.

```
BIGQUERY_EXTRACTOR_ACCESS_TOKEN_JSON=
```


#### 3. Configure tests

##### Create Cloud Storage Bucket
Go to the Google Cloud Console https://console.cloud.google.com/ and select one of your projects.

Project must have enabled:
 - Billing in  **`Navigation menu > Billing`** 
 - BigQuery and Cloud Storage API  in  **`Navigation menu > API & Services > Library`**

In **`Navigation menu`** select **`Storage > Browser`** and create 2 new buckets.

- The first must be located in USA
- The second must be located in European Union

Save your project ID and buckets into `.env` file. Bucket must be specified as  _Google Cloud Storage uri_ in following format: `gs://{bucket-name}`.

```
BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET=
BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET_EU=
BIGQUERY_EXTRACTOR_BILLABLE_GOOGLE_PROJECT=
```

##### Errors test project

For testing some types of errors you need second Google Cloud project, which has BigQuery API disabled (**`Navigation menu > API & Services > Library`**).

Save project ID of the second project into `.env` file.

```
BIGQUERY_EXTRACTOR_NONBILLABLE_GOOGLE_PROJECT=
```

### Run tests

Build application

```
docker-compose build
```

Run test in docker

```
docker-compose run --rm tests
```
