<?php
namespace Keboola\Google\BigQuery\RestApi;

use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Psr7\Response;
use Keboola\Google\BigQuery\Exception\ExtractorException;
use Keboola\Google\BigQuery\Exception\UserException;
use Psr\Log\LoggerInterface;

class Job
{
	private $params = array();

	private $name;

	private $projectId;

	/**
	 * @var LoggerInterface
	 */
	private $logger;

	const API_ENDPOINT = 'https://www.googleapis.com/bigquery/v2/projects/%s/jobs';
	const CACHE_DATASET_ID = 'kbc_extractor';
	const CACHE_DATASET_DESC = 'Keboola Google BigQuery Extractor';

	public function __construct($name, $account, array $params, $projectId, LoggerInterface $logger)
	{
		$this->name = $name;
		$this->params = $params;
		$this->projectId = $projectId;
		$this->accountId = $account;

		$this->logger = $logger;
	}

	/**
	 * Check if dataset exist, if not try create it
	 *
	 * @param Client $client
	 * @return bool|null
	 * @throws ExtractorException
	 */
	private function initDataset(Client $client)
	{
		$datasetExists = null;
		try {
			$url = 'https://www.googleapis.com/bigquery/v2/projects/%s/datasets/%s';
			$url = sprintf($url, $this->projectId, self::CACHE_DATASET_ID);

			$response = $client->request($url, 'GET');


			$responseBody = \GuzzleHttp\json_decode($response->getBody(), true);

			if ($response->getStatusCode() == 200 && !empty($responseBody['selfLink'])) {
				$datasetExists = true;
			}
		} catch (RequestException $e) {
			if ($e->getCode() == 404) {
				$datasetExists = false;
			} else {
				throw $e;
			}
		}

		if (!$datasetExists) {
			$url = 'https://www.googleapis.com/bigquery/v2/projects/%s/datasets';
			$url = sprintf($url, $this->projectId);

			$datasetId = sprintf("%s:%s", $this->projectId, self::CACHE_DATASET_ID);
			$params = array(
				"id" => $datasetId,
				"description" => self::CACHE_DATASET_DESC,
			);

			$this->logger->info(sprintf('%s: Creating dataset "%s"', $this->name, $datasetId));

			$response = $client->request($url, 'POST', ['content-type' => 'application/json'], ["json" => $params]);

			$responseBody = \GuzzleHttp\json_decode($response->getBody(), true);
			if ($response->getStatusCode() != 200 || empty($responseBody['selfLink'])) {
				throw new ExtractorException('Could not create query dataset');
			}

			$datasetExists = true;
			$this->logger->info(sprintf('%s: Dataset "%s" created', $this->name, $datasetId));
		}

		return $datasetExists;
	}

	/**
	 * @param Response $response
	 * @param Client $client
	 * @return bool
	 */
	private function validateJobStatus(Response $response, Client $client)
	{
		$responseBody = \GuzzleHttp\json_decode($response->getBody(), true);

		if (!empty($responseBody['status']['errors'])) {
			foreach ($responseBody['status']['errors'] AS $error) {
				$message = '';
				if (isset($error['location'])) {
					$message .= '[' . mb_strtoupper($error['location']) . '] ';
				}
				if (isset($error['reason'])) {
					$message .= '[' . mb_strtoupper($error['reason']) . '] ';
				}
				if (isset($error['message'])) {
					$message .= $error['message'];
				}
				throw new UserException("Google API Error: " . $message);
			}
		}

		return true;
	}

	public function execute(Client $client)
	{
		$url = sprintf(self::API_ENDPOINT, $this->projectId);

		$this->logStart();

		$response = $client->request($url, 'POST', ['content-type' => 'application/json'], ["json" => $this->params]);

		$this->validateJobStatus($response, $client);

		$responseBody = \GuzzleHttp\json_decode($response->getBody(), true);

		// polling job
		$retriesCount = 0;
		do {
			$waitSeconds = min(pow(2, $retriesCount), 20);
			sleep($waitSeconds);

			try {
				$response = $client->request($responseBody['selfLink'], 'GET');

				$this->validateJobStatus($response, $client);

				$responseBody = \GuzzleHttp\json_decode($response->getBody(), true);
			} catch (RequestException $e) {
				throw $e;
			}

			$this->logger->debug(sprintf('%s: Polling query job', $this->name));

			$retriesCount++;
		} while (in_array($responseBody['status']['state'], array('RUNNING', 'PENDING')));

		$this->logEnd();
		return $responseBody;
	}

	private function logStart()
	{
		$types = array_keys($this->params['configuration']);
		$type = reset($types);

		if ($type == 'query') {
			$this->logger->info(sprintf('%s: Query start', $this->name));
		}

		if ($type == 'extract') {
			$this->logger->info(sprintf('%s: Data extraction start', $this->name));
		}
	}

	private function logEnd()
	{
		$types = array_keys($this->params['configuration']);
		$type = reset($types);

		if ($type == 'query') {
			$this->logger->info(sprintf('%s: Query finished', $this->name));
		}

		if ($type == 'extract') {
			$this->logger->info(sprintf('%s: Data extraction finished', $this->name));
		}
	}

	/**
	 * Build query job, validate destination dataset
	 *
	 * @param $account
	 * @param $config
	 * @param Client $client
	 * @param LoggerInterface $logger
	 * @return Job
	 */
	public static function buildQuery($account, $config, Client $client, LoggerInterface $logger)
	{
		$params = array(
			"configuration" => array(
				"query" => array(
					"flattenResults" => $config['flattenResults'],
					"allowLargeResults" => true,
					"query" => $config['query'],
					"destinationTable" => array(
						'projectId' => $config['projectId'],
						'datasetId' => self::CACHE_DATASET_ID,
						'tableId' => IdGenerator::generateTableName($account, $config),
					),
					"useQueryCache" => true,
					"writeDisposition" => 'WRITE_TRUNCATE' //@TODO maybe alert
				)
			)
		);

		$job = new Job($config['name'], $account, $params, $config['projectId'], $logger);

		$job->initDataset($client);

		return $job;
	}

	/**
	 * Build extract job, validate destionation dataset
	 * @param $account
	 * @param $config
	 * @param Client $client
	 * @param LoggerInterface $logger
	 * @return Job
	 */
	public static function buildExport($account, $config, Client $client, LoggerInterface $logger)
	{
		$format = strtoupper($config['format']);
		if ($format == 'JSON') {
			$format = 'NEWLINE_DELIMITED_JSON';
		}

		$params = array(
			"configuration" => array(
				"extract" => array(
					"compression" => "GZIP",
					"destinationFormat" => $format,
					"sourceTable" => array(
						'projectId' => $config['projectId'],
						'datasetId' => self::CACHE_DATASET_ID,
						'tableId' => IdGenerator::generateTableName($account, $config),
					),
					"printHeader" => true,
					"destinationUris" => array(
						IdGenerator::generateExportPath($account, $config)
					)
				),
			)
		);

		return new Job($config['name'], $account, $params, $config['projectId'], $logger);
	}
}