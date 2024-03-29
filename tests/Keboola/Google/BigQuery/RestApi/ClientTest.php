<?php
namespace Keboola\Google\BigQuery\RestApi;

use GuzzleHttp\Exception\ClientException;
use Monolog\Handler\TestHandler;
use Monolog\Logger;

class ClientTest extends \PHPUnit_Framework_TestCase
{
    /** @var Client */
    private $client;

    /** @var \Monolog\Handler\TestHandler */
    private $testHandler;

    const TEST_DATASET_ID = 'ClientTest';

    private $testProjectId;

    public function setUp()
    {
        parent::setUp();

        $this->testHandler = new TestHandler();
        $logger = new Logger('Google BigQuery API tests');
        $logger->pushHandler($this->testHandler);

        $data = json_decode(BIGQUERY_EXTRACTOR_ACCESS_TOKEN_JSON, true);
        $this->client = new Client(
            BIGQUERY_EXTRACTOR_APP_KEY,
            BIGQUERY_EXTRACTOR_APP_SECRET,
            $data['access_token'],
            $data['refresh_token'],
            $logger
        );

        $this->testProjectId = BIGQUERY_EXTRACTOR_BILLABLE_GOOGLE_PROJECT;

        if ($this->client->datasetExists($this->testProjectId, self::TEST_DATASET_ID)) {
            $this->client->deleteDataset($this->testProjectId, self::TEST_DATASET_ID);
        }
    }

    public function manageDatasetData()
    {
        return [
            'dataset without description' => [
                $this->testProjectId,
                self::TEST_DATASET_ID,
                null,
            ],
            'dataset with description' => [
                $this->testProjectId,
                self::TEST_DATASET_ID,
                'PHP BigQuery test',
            ],
        ];
    }

    public function testManageDataset()
    {
        $projectId = $this->testProjectId;
        $datasetId = self::TEST_DATASET_ID;

        $this->assertFalse($this->client->datasetExists($projectId, $datasetId));

        // create new dataset without description
        $dataset = $this->client->createDataset($projectId, $datasetId);

        $this->assertEquals('bigquery#dataset', $dataset['kind']);
        $this->assertEquals($datasetId, $dataset['datasetReference']['datasetId']);
        $this->assertEquals($projectId, $dataset['datasetReference']['projectId']);
        $this->assertEmpty($dataset['description']);

        $this->assertTrue($this->client->datasetExists($projectId, $datasetId));

        // remove dataset
        $this->assertTrue($this->client->deleteDataset($projectId, $datasetId));
        $this->assertFalse($this->client->datasetExists($projectId, $datasetId));

        // create new dataset with description
        $dataset = $this->client->createDataset($projectId, $datasetId, 'PHP KB BigQuery test');

        $this->assertEquals('bigquery#dataset', $dataset['kind']);
        $this->assertEquals($projectId . ':' . $datasetId, $dataset['id']);
        $this->assertEquals($datasetId, $dataset['datasetReference']['datasetId']);
        $this->assertEquals($projectId, $dataset['datasetReference']['projectId']);
        $this->assertEquals('PHP KB BigQuery test', $dataset['description']);

        $this->assertTrue($this->client->datasetExists($projectId, $datasetId));

        // remove dataset
        $this->assertTrue($this->client->deleteDataset($projectId, $datasetId));
        $this->assertFalse($this->client->datasetExists($projectId, $datasetId));
    }

    public function testListFiles()
    {
        $bucket = BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET;
        $fileInfos = [];
        $account = 'test-list';


        $queries = [
            'first query' => 2,
            'second query' => 1,
        ];

        // file creation
        foreach ($queries as $queryName => $fileCount) {
            $filePath = IdGenerator::generateExportPath(
                $account,
                [
                    'name' => $queryName,
                    'format' => 'csv',
                ],
                [
                    "projectId" => $this->testProjectId,
                    "storage" => $bucket,
                ]
            );

            $filePath = str_replace($bucket, '', $filePath);
            $filePath = trim($filePath, '/');
            $filePath = explode('*', $filePath, 2);
            $filePath = $filePath[0];

            for ($i = 0; $i < $fileCount; $i++) {
                $response = $this->client->request(
                    'https://www.googleapis.com/upload/storage/v1/b/' . str_replace('gs://', '', $bucket) . '/o',
                    'POST',
                    [],
                    [
                        'query' => [
                            'uploadType' => 'media',
                            'name' => $filePath . '-' . $i,
                        ]
                    ]
                );

                $fileInfos[] = \GuzzleHttp\json_decode($response->getBody(), true);
            }
        }

        // files count validation
        foreach ($queries as $queryName => $fileCount) {
            $files = $this->client->listCloudStorageFiles(
                $account,
                [
                    'name' => $queryName,
                    'format' => 'csv',
                ],
                [
                    "projectId" => $this->testProjectId,
                    "storage" => $bucket,
                ]
            );

            $this->assertEquals($fileCount, count($files));
        }
    }

    public function testRetries(): void
    {
        $retriesCount = 5;
        $this->client->setBackoffsCount($retriesCount);
        
        try {
            $this->client->deleteDataset('1111', '1111');
            $this->fail('Delete non-existing dataset in non-existing project should produce error');
        } catch (ClientException $e) {
        }

        for ($i = 1; $i <= $retriesCount; $i++) {
            $this->assertTrue($this->testHandler->hasInfoThatContains(sprintf('Retrying request (%dx)', $i)));
        }
    }
}
