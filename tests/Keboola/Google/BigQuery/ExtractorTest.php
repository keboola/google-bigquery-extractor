<?php


use Keboola\Google\BigQuery\Exception\UserException;
use Keboola\Google\BigQuery\Extractor;
use Keboola\Google\BigQuery\RestApi\Client;
use Keboola\Google\BigQuery\RestApi\IdGenerator;
use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Symfony\Component\Yaml\Yaml;

class ExtractorTest extends \PHPUnit_Framework_TestCase
{
	public function configData()
	{
		return [
			[
				[
					"name" => "Big Query Test",
					"query" => "
									SELECT * FROM [publicdata:samples.natality]
									WHERE [publicdata:samples.natality.year] = 1985
									AND [publicdata:samples.natality.state] = 'FL' LIMIT 10
								",
					"incremental" => true,
					"primaryKey" => ["year", "month", "day"],
				]
			],
			[
				[
					"name" => "Big Query Test with destination",
					"outputTable" => "in.c-tests.tableId",
					"query" => "
									SELECT * FROM [publicdata:samples.natality]
									WHERE [publicdata:samples.natality.year] = 1985
									AND [publicdata:samples.natality.state] = 'FL' LIMIT 10
								",
					"incremental" => true,
					"primaryKey" => ["year", "month", "day"],
				]
			],
			[
				[
					"name" => "Big Query Test disabled",
					"outputTable" => "in.c-tests.tableId",
					"query" => "
									SELECT * FROM [publicdata:samples.natality]
									WHERE [publicdata:samples.natality.year] = 1985
									AND [publicdata:samples.natality.state] = 'FL' LIMIT 10
								",
					"incremental" => true,
					"enabled" => false,
					"primaryKey" => ["year", "month", "day"],
				]
			]
		];
	}

	/**
	 * @dataProvider configData
	 */
	public function testRun($query)
	{
		$this->cleanupExtraction($query);

		$enabled = (!isset($query['enabled']) || $query['enabled'] === true) ? true : false;

		$testHandler = new TestHandler(Logger::INFO);

		$logger = new \Monolog\Logger(APP_NAME, array(
			(new StreamHandler('php://stdout', Logger::INFO))->setFormatter(new LineFormatter("%message%\n")),
			(new StreamHandler('php://stderr', Logger::ERROR))->setFormatter(new LineFormatter("%message%\n")),
			$testHandler,
		));

		$config = [
			"parameters" => [
				"google" => [
					"projectId" => BIGQUERY_EXTRACTOR_BILLABLE_GOOGLE_PROJECT,
					"storage" => BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET,
				],
				"queries" => [$query]
			],
			"authorization" => [
				"oauth_api" => [
					"credentials" => [
						"#data" => BIGQUERY_EXTRACTOR_ACCESS_TOKEN_JSON,
						"appKey" => BIGQUERY_EXTRACTOR_APP_KEY,
						"#appSecret" => BIGQUERY_EXTRACTOR_APP_SECRET,
					]
				]
			]
		];

		$extractor = new Extractor(["logger" => $logger]);
		$extractor->setConfig($config)->run();

		$this->validateCleanup($query, $config['parameters']['google']);
		$this->validateExtraction($query, $config['parameters']['google'], $enabled ? 2 : 0);

		if (!$enabled) {
			$this->validateSkipped($query, $testHandler);
		}
	}

	private function validateSkipped($query, TestHandler $handler)
	{
		$records = $handler->getRecords();

		$this->assertCount(1, $records);

		$skippedQuery = false;
		if (strpos($records[0]['message'], $query['name']) !== false) {
			if (strpos($records[0]['message'], 'Skipped') !== false) {
				$skippedQuery = true;
			}
		}

		$this->assertTrue($skippedQuery);
	}

	public function testListProjects()
	{
		$logger = new \Monolog\Logger(APP_NAME, array(
			(new StreamHandler('php://stdout', Logger::INFO))->setFormatter(new LineFormatter("%message%\n")),
			(new StreamHandler('php://stderr', Logger::ERROR))->setFormatter(new LineFormatter("%message%\n")),
		));

		$config = [
			"action" => "listProjects",
			"parameters" => [
				"google" => [
					"projectId" => BIGQUERY_EXTRACTOR_BILLABLE_GOOGLE_PROJECT,
					"storage" => BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET,
				],
			],
			"authorization" => [
				"oauth_api" => [
					"credentials" => [
						"#data" => BIGQUERY_EXTRACTOR_ACCESS_TOKEN_JSON,
						"appKey" => BIGQUERY_EXTRACTOR_APP_KEY,
						"#appSecret" => BIGQUERY_EXTRACTOR_APP_SECRET,
					]
				]
			]
		];

		$extractor = new Extractor(["logger" => $logger]);
		$result = $extractor->setConfig($config)->run();

		$this->assertArrayHasKey('status', $result);
		$this->assertArrayHasKey('projects', $result);

		$this->assertTrue(count($result['projects']) > 0);

		foreach ($result['projects'] AS $metaData) {
			$this->assertArrayHasKey('id', $metaData);
			$this->assertArrayHasKey('name', $metaData);
		}

		$this->assertEquals('success', $result['status']);
	}

	public function testInvalidAction()
	{
		$logger = new \Monolog\Logger(APP_NAME, array(
			(new StreamHandler('php://stdout', Logger::INFO))->setFormatter(new LineFormatter("%message%\n")),
			(new StreamHandler('php://stderr', Logger::ERROR))->setFormatter(new LineFormatter("%message%\n")),
		));

		$config = [
			"action" => "invalid",
			"parameters" => [
				"google" => [
					"projectId" => BIGQUERY_EXTRACTOR_BILLABLE_GOOGLE_PROJECT,
					"storage" => BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET,
				],
			],
			"authorization" => [
				"oauth_api" => [
					"credentials" => [
						"#data" => BIGQUERY_EXTRACTOR_ACCESS_TOKEN_JSON,
						"appKey" => BIGQUERY_EXTRACTOR_APP_KEY,
						"#appSecret" => BIGQUERY_EXTRACTOR_APP_SECRET,
					]
				]
			]
		];

		$extractor = new Extractor(["logger" => $logger]);
		$extractor->setConfig($config);

		try {
			$extractor->run();

			$this->fail("Calling nonexisting action should produce error");
		} catch (UserException $e) {

		}
	}

	private function cleanupExtraction($query)
	{
		$dirPath = getenv('KBC_DATADIR') . '/out/tables';

		if (!is_dir($dirPath)) {
			return;
		}

		$files = array_map(
			function ($fileName) use ($dirPath) {
				return $dirPath . '/' . $fileName;
			},
			array_filter(
				scandir($dirPath),
				function ($fileName) use ($dirPath, $query) {
					$filePath = $dirPath . '/' . $fileName;
					if (!is_file($filePath)) {
						return false;
					}

					return strpos($fileName, IdGenerator::generateFileName(getenv('KBC_CONFIGID'), $query)) !== false;
				}
			)
		);

		foreach ($files AS $file) {
			unlink($file);
		}
	}

	private function validateExtraction($query, $project, $expectedFiles = 2)
	{
		$dirPath = getenv('KBC_DATADIR') . '/out/tables';

		$files = array_map(
			function ($fileName) use ($dirPath) {
				return $dirPath . '/' . $fileName;
			},
			array_filter(
				scandir($dirPath),
				function ($fileName) use ($dirPath, $query) {
					$filePath = $dirPath . '/' . $fileName;
					if (!is_file($filePath)) {
						return false;
					}

					return strpos($fileName, IdGenerator::generateFileName(getenv('KBC_CONFIGID'), $query)) !== false;
				}
			)
		);

		$this->assertCount($expectedFiles, $files);

		if($expectedFiles < 1) {
			return;
		}

		$manifestValidated = false;
		$csvValidated = false;

		foreach ($files AS $file) {
			// manifest validation
			if (preg_match('/.manifest$/ui', $file)) {
				$params = Yaml::parse(file_get_contents($file));

				$this->assertArrayHasKey('destination', $params);
				$this->assertArrayHasKey('incremental', $params);
				$this->assertArrayHasKey('primary_key', $params);

				$this->assertTrue($params['incremental']);
				$this->assertEquals($query['primaryKey'], $params['primary_key']);

				$this->assertEquals(IdGenerator::generateOutputTableId(getenv('KBC_CONFIGID'), $query), $params['destination']);

				if (isset($query['outputTable'])) {
					$this->assertEquals($query['outputTable'], $params['destination']);
				}

				$manifestValidated = true;
			}

			// archive validation
			if (preg_match('/.csv.gz$/ui', $file)) {

				exec("gunzip -d " . escapeshellarg($file), $output, $return);
				$this->assertEquals(0, $return);

				$csvValidated = true;
			}
		}

		$this->assertTrue($manifestValidated);
		$this->assertTrue($csvValidated);

	}

	private function validateCleanup($query, $project)
	{
		// validation of clenaup
		$client = new Client(BIGQUERY_EXTRACTOR_APP_KEY, BIGQUERY_EXTRACTOR_APP_SECRET);

		$data = json_decode(BIGQUERY_EXTRACTOR_ACCESS_TOKEN_JSON, true);
		$client->setCredentials($data['access_token'], $data['refresh_token']);

		$files = $client->listCloudStorageFiles(getenv('KBC_CONFIGID'), $query, $project);
		$this->assertCount(0, $files);
		return true;
	}

	public function testListFiles()
	{
		// validation of clenaup
		$client = new Client(BIGQUERY_EXTRACTOR_APP_KEY, BIGQUERY_EXTRACTOR_APP_SECRET);

		$data = json_decode(BIGQUERY_EXTRACTOR_ACCESS_TOKEN_JSON, true);
		$client->setCredentials($data['access_token'], $data['refresh_token']);

		$fileInfos = [];
		$account = 'test-list';


		$queries = [
			'first query' => 2,
			'second query' => 1,
		];

		// file creation
		foreach ($queries AS $queryName => $fileCount) {
			$filePath = IdGenerator::generateExportPath(
				$account,
				[
					'name' => $queryName,
					'format' => 'csv',
				],
				[
					"projectId" => BIGQUERY_EXTRACTOR_BILLABLE_GOOGLE_PROJECT,
					"storage" => BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET,
				]
			);

			$filePath = str_replace(BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET, '', $filePath);
			$filePath = trim($filePath, '/');
			$filePath = explode('*', $filePath, 2);
			$filePath = $filePath[0];

			for ($i = 0; $i < $fileCount; $i++) {
				$response = $client->request(
					'https://www.googleapis.com/upload/storage/v1/b/' . str_replace('gs://', '', BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET) . '/o',
					'POST',
					[],
					[
						'query' => [
							'uploadType' => 'media',
							'name' => $filePath . '-' . $i,
						]
					]
				);

				$fileInfos[] = json_decode($response->getBody()->getContents(), true);
			}


		}

		// files count validation
		foreach ($queries AS $queryName => $fileCount) {
			$files = $client->listCloudStorageFiles(
				$account,
				[
					'name' => $queryName,
					'format' => 'csv',
				],
				[
					"projectId" => BIGQUERY_EXTRACTOR_BILLABLE_GOOGLE_PROJECT,
					"storage" => BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET,
				]
			);

			$this->assertEquals($fileCount, count($files));
		}

		return true;
	}
}