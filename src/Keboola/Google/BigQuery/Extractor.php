<?php
namespace Keboola\Google\BigQuery;

use GuzzleHttp\Exception\RequestException;
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

    private $action;

    const DEFAULT_ACTION = 'run';

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

        if (isset($params['action'])) {
            $this->action = $params['action'];
        } else {
            $this->action = self::DEFAULT_ACTION;
        }

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
                new ParamsDefinition($this->action),
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

        $method = sprintf('process%sAction', ucfirst($this->action));
        if (!method_exists($this, $method)) {
            throw new UserException(sprintf("Action '%s' does not exist.", $this->action));
        }

        return $this->$method();
    }

    /**
     *
     * @param RequestException $e
     */
    private function processRequestException(RequestException $e)
    {
        if ($e->getResponse() && $e->getResponse()->getStatusCode() < 500) {
            $responseBody = \GuzzleHttp\json_decode($e->getResponse()->getBody(), true);

            if (!empty($responseBody['error']['errors'])) {
                foreach ($responseBody['error']['errors'] as $error) {
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

            if (!empty($responseBody['error']) && is_string($responseBody['error'])) {
                if (isset($responseBody['error_description'])) {
                    $message = sprintf('%s (%s)', $responseBody['error_description'], $responseBody['error']);
                } else {
                    $message = $responseBody['error'];
                }

                if (strpos($message, 'invalid_grant') !== false) {
                    $message .= ' - Try re-authorize your account';
                }

                throw new UserException("Google API Error: " . $message);
            }
        }

        throw $e;
    }

    private function processListBucketsAction()
    {
        $google = $this->initGoogle();
        $project = $this->params['parameters']['google'];

        try {
            return [
                'status' => 'success',
                'buckets' => $google->listBuckets($project),
            ];
        } catch (RequestException $e) {
            $this->processRequestException($e);
        }
    }

    private function processListProjectsAction()
    {
        $google = $this->initGoogle();

        try {
            return [
                'status' => 'success',
                'projects' => $google->listProjects(),
            ];
        } catch (RequestException $e) {
            $this->processRequestException($e);
        }
    }

    private function processRunAction()
    {
        $google = $this->initGoogle();
        $project = $this->params['parameters']['google'];

        $dirPath = getenv('KBC_DATADIR') . '/out/tables';
        if (!file_exists($dirPath) || !is_dir($dirPath)) {
            mkdir($dirPath, 0770, true);
        }

        foreach ($this->params['parameters']['queries'] as $query) {
            if ($query['enabled'] !== true) {
                $this->logger->info(sprintf('%s: Skipped', $query["name"]));
            } else {
                $query["format"] = "csv";
                // execute query
                $job = Job::buildQuery(
                    getenv('KBC_CONFIGID'),
                    $query,
                    $project,
                    $google,
                    $this->logger
                );

                $job->execute($google);

//            @FIXME each job should log result to info

                // export to csv
                $job = Job::buildExport(
                    getenv('KBC_CONFIGID'),
                    $query,
                    $project,
                    $google,
                    $this->logger
                );
                $job->execute($google);


                $result = $google->listCloudStorageFiles(getenv('KBC_CONFIGID'), $query, $project);
                $this->logger->info(sprintf('%s: Starting download of %s files', $query["name"], count($result)));

                // create directory for sliced files
                $outputTable = IdGenerator::generateOutputTableId(getenv('KBC_CONFIGID'), $query);
                $outputDataDir = $dirPath . '/' . $outputTable . ".csv.gz";
                if (!file_exists($outputDataDir) || !is_dir($outputDataDir)) {
                    mkdir($outputDataDir, 0770, true);
                }

                // download files
                foreach ($result as $cloudFileInfo) {
                    $fileName = $cloudFileInfo['name'];
                    $fileName =  explode('/', $fileName);
                    $fileName = $fileName[count($fileName) -1];

                    $filePath = $outputDataDir . '/' . $fileName;
                    $resource = fopen($filePath, 'w');

                    $google->request($cloudFileInfo['mediaLink'], 'GET', [], ['sink' => $resource]);

                    $this->logger->info(sprintf('%s: %s downloaded', $query["name"], $fileName));
                }

                // create manifest
                $tableId = IdGenerator::generateTableName(getenv('KBC_CONFIGID'), $query);
                $manifest = [
                    'destination' => $outputTable,
                    'delimiter' => ',',
                    'enclosure' => '"',
                    'primary_key' => $query['primaryKey'],
                    'incremental' => $query['incremental'],
                    'columns' => array_map(
                        function ($tableColumn) {
                            return $tableColumn['name'];
                        },
                        $google->listTableColumns($project['projectId'], IdGenerator::genereateExtractorDataset($project['location']), $tableId)
                    )
                ];

                file_put_contents($outputDataDir . '.manifest', Yaml::dump($manifest));
                $this->logger->info(sprintf('%s: Manifest created', $query["name"]));

                // cloud storage cleanup
                if (!count($result)) {
                    $this->logger->info(sprintf('%s: Any Cloud Storage cleanup needed', $query["name"]));
                    continue;
                }

                $this->logger->info(sprintf('%s: Cloud Storage cleanup start (%s files)', $query["name"], count($result)));

                foreach ($result as $cloudFileInfo) {
                    $fileName = $cloudFileInfo['name'];
                    $fileName =  explode('/', $fileName);
                    $fileName = $fileName[count($fileName) -1];

                    if ($google->deleteCloudStorageFile($cloudFileInfo)) {
                        $this->logger->info(sprintf('%s: File %s removed', $query["name"], $fileName));
                    } else {
                        $this->logger->error(sprintf('%s: File %s was not removed', $query["name"], $fileName));
                        //@FIXME log error response
                        throw new UserException("Cloud Storage file was not removed");
                    }
                }

                $this->logger->info(sprintf('%s: Cloud Storage cleanup finished', $query["name"]));
            }
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
