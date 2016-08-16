<?php
namespace Keboola\Google\BigQuery;

use Keboola\Google\BigQuery\Configuration\AuthorizationDefinition;
use Keboola\Google\BigQuery\Configuration\ParamsDefinition;
use Keboola\Google\BigQuery\Exception\UserException;
use Keboola\Google\BigQuery\RestApi\Client;
use Keboola\Google\BigQuery\RestApi\IdGenerator;
use Keboola\Google\BigQuery\RestApi\Job;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Config\Definition\Processor;
use Symfony\Component\Config\Definition\Exception\Exception as ConfigException;
use Symfony\Component\Yaml\Yaml;

class Extractor
{
	/**
	 * @var LoggerInterface
	 */
	private $logger;

	private $params = [];

	public function __construct(array $options = [])
	{
		if (isset($options['logger']) && $options['logger'] instanceof LoggerInterface) {
			$this->logger = $options['logger'];
		} else {
			$this->logger = new NullLogger();
		}
	}

	/**
	 * Set and validate configuration parameters
	 *
	 * @param $params
	 * @return $this
	 * @throws UserException
	 */
	public function setConfig($params)
	{
		$this->params = [];

		if (!isset($params['authorization'])) {
			throw new UserException(UserException::ERR_MISSING_OAUTH_CONFIG);
		}

		if (!isset($params['parameters'])) {
			throw new UserException(UserException::ERR_MISSING_PARAMS_CONFIG);
		}

		// oauth validation
		try {
			$processor = new Processor();
			$processedParameters = $processor->processConfiguration(
				new AuthorizationDefinition(),
				[$params['authorization']]
			);

			// tokens
			$token = json_decode($processedParameters['oauth_api']['credentials']['#data'], true);
			if (!isset($token['access_token']) || !isset($token['refresh_token'])) {
				throw new ConfigException('Missing access or refresh token data');
			}

			$processedParameters['oauth_api']['credentials']['#data'] = $token;
			$this->params['authorization'] = $processedParameters;
		} catch (ConfigException $e) {
			$this->logger->error($e->getMessage(), []);
			throw new UserException(UserException::ERR_OAUTH_CONFIG);
		}

		// params validation
		try {
			$processor = new Processor();
			$processedParameters = $processor->processConfiguration(
				new ParamsDefinition(),
				[$params['parameters']]
			);

			$this->params['parameters'] = $processedParameters;
		} catch (ConfigException $e) {
			throw new UserException($e->getMessage());
		}

		return $this;
	}

	/**
	 * Run extractor
	 * @return null
	 * @throws \Exception
	 */
	public function run()
	{
		if (empty($this->params)) {
			throw new \RuntimeException("Missing configuration");
		}

		return $this->processRunAction();
	}

	private function processRunAction()
	{
		$google = $this->initGoogle();

		$dirPath = getenv('KBC_DATADIR') . '/out/tables';
		if (!file_exists($dirPath) || !is_dir($dirPath)) {
			mkdir($dirPath, 0755, true);
		}

		foreach ($this->params['parameters']['queries'] AS $query) {
			$query["format"] = "csv";
			// execute query
			$job = Job::buildQuery(
				getenv('KBC_CONFIGID'),
				$query,
				$google,
				$this->logger
			);

			$job->execute($google);

//			@FIXME each job should log result to info

			// export to csv
			$job = Job::buildExport(
				getenv('KBC_CONFIGID'),
				$query,
				$google,
				$this->logger
			);
			$job->execute($google);


			$result = $google->listCloudStorageFiles(getenv('KBC_CONFIGID'), $query);
			$this->logger->info(sprintf('%s: Starting download of %s files', $query["name"], count($result)));


			foreach ($result AS $cloudFileInfo) {
				$fileName = $cloudFileInfo['name'];
				$fileName =  explode('/', $fileName);
				$fileName = $fileName[count($fileName) -1];

				$response = $google->request($cloudFileInfo['mediaLink']);

				$filePath = $dirPath . '/' . $fileName;

				file_put_contents($filePath, $response->getBody());

				$manifest = [
					'destination' => 'in.c-test-01.' . IdGenerator::generateFileName(getenv('KBC_CONFIGID'), $query),
					'delimiter' => ',',
					'enclosure' => '',
					'primary_key' => $query['primaryKey'],
					'incremental' => $query['incremental'],

				];

				file_put_contents($filePath . '.manifest', Yaml::dump($manifest));
				$this->logger->info(sprintf('%s: %s downloaded', $query["name"], $fileName));
			}

			if (!count($result)) {
				$this->logger->info(sprintf('%s: Any Cloud Storage cleanup needed', $query["name"]));
				continue;
			}

			$this->logger->info(sprintf('%s: Cloud Storage cleanup start (%s files)', $query["name"], count($result)));

			foreach ($result AS $cloudFileInfo) {
				$fileName = $cloudFileInfo['name'];
				$fileName =  explode('/', $fileName);
				$fileName = $fileName[count($fileName) -1];

				if ($google->deleteCloudStorageFile($cloudFileInfo)) {
					$this->logger->info(sprintf('%s: File %s removed', $query["name"], $fileName));
				} else {
					$this->logger->error(sprintf('%s: File %s was not removed', $query["name"], $fileName));
					//@FIXME log error response
					throw new UserException("Cloud Storage file was not removed"/*, $googleApi->getLastRequest(), $googleApi->getLastResponse()*/);
				}
			}

			$this->logger->info(sprintf('%s: Cloud Storage cleanup finished', $query["name"]));
		}

		return null;
	}

	/**
	 * @return Client
	 */
	private function initGoogle()
	{
		$restApi =  new Client(
			$this->params['authorization']['oauth_api']['credentials']['appKey'],
			$this->params['authorization']['oauth_api']['credentials']['#appSecret']
		);

		$restApi->setCredentials(
			$this->params['authorization']['oauth_api']['credentials']['#data']['access_token'],
			$this->params['authorization']['oauth_api']['credentials']['#data']['refresh_token']
		);

		return $restApi;
	}
}