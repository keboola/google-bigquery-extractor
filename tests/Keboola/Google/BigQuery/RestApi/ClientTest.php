<?php
namespace Keboola\Google\BigQuery\RestApi;

class ClientTest extends \PHPUnit_Framework_TestCase
{
    /** @var Client */
    private $client;

    const TEST_DATASET_ID = 'ClientTest';

    private $testProjectId;

    public function setUp()
    {
        parent::setUp();

        $client = new Client(BIGQUERY_EXTRACTOR_APP_KEY, BIGQUERY_EXTRACTOR_APP_SECRET);

        $data = json_decode(BIGQUERY_EXTRACTOR_ACCESS_TOKEN_JSON, true);
        $client->setCredentials($data['access_token'], $data['refresh_token']);

        $this->client = $client;
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
}
