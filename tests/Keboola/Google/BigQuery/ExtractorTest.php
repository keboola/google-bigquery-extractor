<?php
namespace Keboola\Google\BigQuery\Configuration;

use Keboola\Google\BigQuery\Extractor;
use Keboola\Google\BigQuery\RestApi\Client;
use Keboola\Google\BigQuery\RestApi\IdGenerator;
use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Symfony\Component\Yaml\Yaml;

class ExtractorTest extends \PHPUnit_Framework_TestCase
{
	public function configData()
	{
		return [
			[
				[
					"name" => "ex-google-bigquery-test",
					"query" => "
									SELECT * FROM [publicdata:samples.natality]
									WHERE [publicdata:samples.natality.year] = 1985
									AND [publicdata:samples.natality.state] = 'FL' LIMIT 10
								",
					"storage" => BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET,
					"projectId" => BIGQUERY_EXTRACTOR_BILLABLE_GOOGLE_PROJECT,
					"incremental" => true,
					"primaryKey" => ["year", "month", "day"],
				]
			]
		];
	}

	/**
	 * @dataProvider configData
	 */
	public function testExtractor($query)
	{
		$this->cleanupExtraction($query);

		$logger = new \Monolog\Logger(APP_NAME, array(
			(new StreamHandler('php://stdout', Logger::INFO))->setFormatter(new LineFormatter("%message%\n")),
			(new StreamHandler('php://stderr', Logger::ERROR))->setFormatter(new LineFormatter("%message%\n")),
		));

		$config = [
			"parameters" => [
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

		$this->validateCleanup($query);
		$this->validateExtraction($query);
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

	private function validateExtraction($query)
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

		$this->assertCount(2, $files);

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
				$this->assertTrue($params['incremental']);

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

	private function validateCleanup($query)
	{
		// validation of clenaup
		$client = new Client(BIGQUERY_EXTRACTOR_APP_KEY, BIGQUERY_EXTRACTOR_APP_SECRET);

		$data = json_decode(BIGQUERY_EXTRACTOR_ACCESS_TOKEN_JSON, true);
		$client->setCredentials($data['access_token'], $data['refresh_token']);

		$files = $client->listCloudStorageFiles(getenv('KBC_CONFIGID'), $query);
		$this->assertCount(0, $files);
		return true;
	}
}