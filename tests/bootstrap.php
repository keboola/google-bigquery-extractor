<?php

require __DIR__ . '/../vendor/autoload.php';


$appKey = getenv('ENV_BIGQUERY_EXTRACTOR_APP_KEY');
$appSecret = getenv('ENV_BIGQUERY_EXTRACTOR_APP_SECRET');
$accessTokenJson = getenv('ENV_BIGQUERY_EXTRACTOR_ACCESS_TOKEN_JSON');

$cloudStorageBucket = getenv('ENV_BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET');
$projectId = getenv('ENV_BIGQUERY_EXTRACTOR_BILLABLE_GOOGLE_PROJECT');

foreach ([$appKey, $appSecret, $accessTokenJson, $cloudStorageBucket, $projectId] as $var) {
	if ($var === false) {
		echo 'Set all required environment variables' . "\n";
		exit(1);
	}
}

define('BIGQUERY_EXTRACTOR_APP_KEY', $appKey);
define('BIGQUERY_EXTRACTOR_APP_SECRET', $appSecret);
define('BIGQUERY_EXTRACTOR_ACCESS_TOKEN_JSON', $accessTokenJson);

define('BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET', $cloudStorageBucket);
define('BIGQUERY_EXTRACTOR_BILLABLE_GOOGLE_PROJECT', $projectId);

