<?php
namespace Keboola\Google\BigQuery\RestApi;

use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Psr7\Response;
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
        }

        throw $e;
    }

    /**
     * Check if dataset exist, if not try create it
     */
    private function initDataset(Client $client)
    {
        try {
            if (!$client->datasetExists($this->projectId, self::CACHE_DATASET_ID)) {
                $this->logger->info(sprintf(
                    '%s: Creating dataset "%s:%s"',
                    $this->name,
                    $this->projectId,
                    self::CACHE_DATASET_ID
                ));

                $datasetMetadata = $client->createDataset(
                    $this->projectId,
                    self::CACHE_DATASET_ID,
                    self::CACHE_DATASET_DESC
                );

                $this->logger->info(sprintf('%s: Dataset "%s" created', $this->name, $datasetMetadata['id']));
            }
        } catch (RequestException $e) {
            $this->processRequestException($e);
        }
    }

    /**
     * @param Response $response
     * @param Client $client
     * @return bool
     */
    private function validateStatus(Response $response, Client $client)
    {
        $responseBody = \GuzzleHttp\json_decode($response->getBody(), true);

        if (!empty($responseBody['error']['errors'])) {
            foreach ($responseBody['error']['errors'] as $error) {
                $message = '';
                if (isset($error['domain'])) {
                    $message .= '[' . mb_strtoupper($error['domain']) . '] ';
                }
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

    /**
     * @param Response $response
     * @param Client $client
     * @return bool
     */
    private function validateJobStatus(Response $response, Client $client)
    {
        $responseBody = \GuzzleHttp\json_decode($response->getBody(), true);

        if (!empty($responseBody['status']['errors'])) {
            foreach ($responseBody['status']['errors'] as $error) {
                $message = '';
                if (isset($error['domain'])) {
                    $message .= '[' . mb_strtoupper($error['domain']) . '] ';
                }
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

        try {
            $response = $client->request($url, 'POST', ['content-type' => 'application/json'], ["json" => $this->params]);
        } catch (RequestException $e) {
            $response = $e->getResponse();
        }

        $this->validateStatus($response, $client);


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
                $this->processRequestException($e);
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
     * @param $project
     * @param Client $client
     * @param LoggerInterface $logger
     * @return Job
     */
    public static function buildQuery($account, $config, $project, Client $client, LoggerInterface $logger)
    {
        $params = array(
            "configuration" => array(
                "query" => array(
                    "flattenResults" => $config['flattenResults'],
                    "useLegacySql" => $config['useLegacySql'],
                    "allowLargeResults" => true,
                    "query" => $config['query'],
                    "destinationTable" => array(
                        'projectId' => $project['projectId'],
                        'datasetId' => self::CACHE_DATASET_ID,
                        'tableId' => IdGenerator::generateTableName($account, $config),
                    ),
                    "useQueryCache" => true,
                    "writeDisposition" => 'WRITE_TRUNCATE'
                )
            )
        );

        $job = new Job($config['name'], $account, $params, $project['projectId'], $logger);

        $job->initDataset($client);

        return $job;
    }

    /**
     * Build extract job, validate destionation dataset
     * @param $account
     * @param $config
     * @param $project
     * @param Client $client
     * @param LoggerInterface $logger
     * @return Job
     */
    public static function buildExport($account, $config, $project, Client $client, LoggerInterface $logger)
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
                        'projectId' => $project['projectId'],
                        'datasetId' => self::CACHE_DATASET_ID,
                        'tableId' => IdGenerator::generateTableName($account, $config),
                    ),
                    "printHeader" => false,
                    "destinationUris" => array(
                        IdGenerator::generateExportPath($account, $config, $project)
                    )
                ),
            )
        );

        return new Job($config['name'], $account, $params, $project['projectId'], $logger);
    }
}
