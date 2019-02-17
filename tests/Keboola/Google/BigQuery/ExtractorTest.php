<?php
namespace Keboola\Google\BigQuery;

use Keboola\Google\BigQuery\Exception\UserException;
use Keboola\Google\BigQuery\RestApi\Client;
use Keboola\Google\BigQuery\RestApi\IdGenerator;
use Keboola\Google\BigQuery\RestApi\Job;
use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Symfony\Component\Yaml\Yaml;

class ExtractorTest extends \PHPUnit_Framework_TestCase
{
    /** @var Client */
    private $client;

    /**
     * Cleanup workspace
     */
    public function setUp()
    {
        $client = new Client(BIGQUERY_EXTRACTOR_APP_KEY, BIGQUERY_EXTRACTOR_APP_SECRET);

        $data = json_decode(BIGQUERY_EXTRACTOR_ACCESS_TOKEN_JSON, true);
        $client->setCredentials($data['access_token'], $data['refresh_token']);

        $url = 'https://www.googleapis.com/storage/v1/b/' . str_replace('gs://', '', BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET) . '/o';

        $params = array(
            'fields' => 'items(mediaLink,id,name,bucket),nextPageToken,prefixes',
        );

        $pageToken = null;

        while ($pageToken !== false) {
            $params['maxResults'] = Client::PAGING;
            $params['pageToken'] = $pageToken;

            $response = $client->request($url, 'GET', [], ['query' => $params]);

            $response = \GuzzleHttp\json_decode($response->getBody(), true);
            if (!array_key_exists('nextPageToken', $response)) {
                $pageToken = false;
            } else {
                $pageToken = $response['nextPageToken'];
            }

            if (!empty($response['items'])) {
                foreach ($response['items'] as $file) {
                    $client->deleteCloudStorageFile($file);
                }
            }
        }

        $this->client = $client;
    }

    public function listProjectsConfigData()
    {
        return [
            [
                [
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
                ]
            ],
            [
                [
                    "action" => "listProjects",
                    "parameters" => [
                        "google" => [
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
                ]
            ],
            [
                [
                    "action" => "listProjects",
                    "parameters" => [
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
                ]
            ]
        ];
    }

    public function listBucketsConfigData()
    {
        return [
            [
                [
                    "action" => "listBuckets",
                    "parameters" => [
                        "google" => [
                            "projectId" => BIGQUERY_EXTRACTOR_BILLABLE_GOOGLE_PROJECT,
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
                ]
            ]
        ];
    }

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
            ],
            [
                [
                    "name" => "Big Query Test using standard SQL",
                    "outputTable" => "in.c-tests.tableId",
                    "query" => "
                                    SELECT * FROM `publicdata.samples.natality`
                                    WHERE year = 1985
                                    AND state = 'FL' LIMIT 10
                                ",
                    "incremental" => true,
                    "enabled" => true,
                    "useLegacySql" => false,
                    "primaryKey" => ["year", "month", "day"],
                ]
            ]
        ];
    }

    public function testActionsUserError()
    {
        // list buckets action
        $config = [
            "action" => "listBuckets",
            "parameters" => [
                "google" => [
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

        $extractor = new Extractor();

        try {
            $result = $extractor->setConfig($config)->run();
            $this->fail("Config without project specification should produce error");
        } catch (UserException $e) {
        }

        // list buckets action
        $config = [
            "action" => "listProjects",
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

        $extractor = new Extractor();

        try {
            $extractor->setConfig($config)->run();
            $this->fail("Config without params section should produce error");
        } catch (UserException $e) {
        }

        // invalid access token
        $data = json_decode(BIGQUERY_EXTRACTOR_ACCESS_TOKEN_JSON, true);
        $data['access_token'] = uniqid();
        $data['refresh_token'] = uniqid();

        $config = [
            "action" => "listProjects",
            "parameters" => [
                "google" => [
                    "projectId" => BIGQUERY_EXTRACTOR_BILLABLE_GOOGLE_PROJECT,
                ],
            ],
            "authorization" => [
                "oauth_api" => [
                    "credentials" => [
                        "#data" => json_encode($data),
                        "appKey" => BIGQUERY_EXTRACTOR_APP_KEY,
                        "#appSecret" => BIGQUERY_EXTRACTOR_APP_SECRET,
                    ]
                ]
            ]
        ];

        $extractor = new Extractor();

        try {
            $extractor->setConfig($config)->run();
            $this->fail("Config without params section should produce error");
        } catch (UserException $e) {
            $this->assertContains('Try re-authorize your account', $e->getMessage());
        }
    }

    public function testRunUserError()
    {
        $query = [
            "name" => "Big Query Test",
            "enabled" => true,
            "query" => "
                                    SELECT * FROM [publicdata:samples.natality]
                                    WHERE [publicdata:samples.natality.year] = 1985
                                    AND [publicdata:samples.natality.state] = 'FL' LIMIT 10
                                ",
            "incremental" => true,
            "primaryKey" => ["year", "month", "day"],
        ];

        $this->cleanupExtraction();

        $testHandler = new TestHandler(Logger::INFO);

        $logger = new \Monolog\Logger(APP_NAME, array(
            (new StreamHandler('php://stdout', Logger::INFO))->setFormatter(new LineFormatter("%message%\n")),
            (new StreamHandler('php://stderr', Logger::ERROR))->setFormatter(new LineFormatter("%message%\n")),
            $testHandler,
        ));

        // non billable project
        $config = [
            "parameters" => [
                "google" => [
                    "projectId" => BIGQUERY_EXTRACTOR_NONBILLABLE_GOOGLE_PROJECT,
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

        try {
            $extractor->setConfig($config)->run();
            $this->fail("Config with non-billable project should produce error");
        } catch (UserException $e) {
        }

        // non existing cloud storage bucket
        $config = [
            "parameters" => [
                "google" => [
                    "projectId" => BIGQUERY_EXTRACTOR_BILLABLE_GOOGLE_PROJECT,
                    "storage" => BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET . '-' . uniqid(),
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

        try {
            $extractor->setConfig($config)->run();
            $this->fail("Config with non existing path should produce error");
        } catch (UserException $e) {
        }

        // invalid cloud storage bucket path
        $config = [
            "parameters" => [
                "google" => [
                    "projectId" => BIGQUERY_EXTRACTOR_BILLABLE_GOOGLE_PROJECT,
                    "storage" => str_replace('gs://', '', BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET),
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

        try {
            $extractor->setConfig($config)->run();
            $this->fail("Config with invalid path should produce error");
        } catch (UserException $e) {
        }
    }

    /**
     * @dataProvider configData
     */
    public function testRun($query)
    {
        $projectId = BIGQUERY_EXTRACTOR_BILLABLE_GOOGLE_PROJECT;
        $this->cleanupExtraction();

        $datasetId = IdGenerator::genereateExtractorDataset('US');

        if ($this->client->datasetExists($projectId, $datasetId)) {
            $this->cleanupDataset($projectId, $datasetId);
            $this->client->deleteDataset($projectId, $datasetId);
        }

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

    /**
     * @dataProvider listProjectsConfigData
     */
    public function testListProjects($params)
    {
        // without optional params
        $logger = new \Monolog\Logger(APP_NAME, array(
            (new StreamHandler('php://stdout', Logger::INFO))->setFormatter(new LineFormatter("%message%\n")),
            (new StreamHandler('php://stderr', Logger::ERROR))->setFormatter(new LineFormatter("%message%\n")),
        ));

        $extractor = new Extractor(["logger" => $logger]);
        $result = $extractor->setConfig($params)->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('projects', $result);

        $this->assertTrue(count($result['projects']) > 0);

        foreach ($result['projects'] as $metaData) {
            $this->assertArrayHasKey('id', $metaData);
            $this->assertArrayHasKey('name', $metaData);
        }

        $this->assertEquals('success', $result['status']);
    }


    /**
     * @dataProvider listBucketsConfigData
     */
    public function testListBuckets($params)
    {
        $logger = new \Monolog\Logger(APP_NAME, array(
            (new StreamHandler('php://stdout', Logger::INFO))->setFormatter(new LineFormatter("%message%\n")),
            (new StreamHandler('php://stderr', Logger::ERROR))->setFormatter(new LineFormatter("%message%\n")),
        ));

        $extractor = new Extractor(["logger" => $logger]);
        $result = $extractor->setConfig($params)->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('buckets', $result);

        $this->assertTrue(count($result['buckets']) > 0);

        $testBucketFound = false;
        $testBucketId = BIGQUERY_EXTRACTOR_CLOUD_STORAGE_BUCKET;
        if (strpos($testBucketId, 'gs://') !== false) {
            $testBucketId = str_replace('gs://', '', $testBucketId);
        }

        foreach ($result['buckets'] as $metaData) {
            $this->assertArrayHasKey('id', $metaData);
            $this->assertArrayHasKey('name', $metaData);

            if ($metaData['id'] === $testBucketId) {
                $testBucketFound = true;
            }
        }

        $this->assertEquals('success', $result['status']);
        $this->assertTrue($testBucketFound);
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

    private function cleanupDataset(string $projectId, string $datasetId)
    {
        $google = new \Google_Client([
            'client_id' => BIGQUERY_EXTRACTOR_APP_KEY,
            'client_secret' => BIGQUERY_EXTRACTOR_APP_SECRET,
        ]);

        $google->setAccessToken(BIGQUERY_EXTRACTOR_ACCESS_TOKEN_JSON);

        $tables = (new \Google_Service_Bigquery($google))->tables;

        foreach ($tables->listTables($projectId, $datasetId) as $table) {
            $tables->delete(
                $projectId,
                $datasetId,
                $table->getTableReference()->getTableId()
            );
        }
    }

    private function cleanupExtraction()
    {
        $dirPath = new \SplFileInfo(getenv('KBC_DATADIR') . '/out/tables');
        if ($dirPath->isDir()) {
            $dirIterator = new \RecursiveDirectoryIterator($dirPath, \FilesystemIterator::SKIP_DOTS);
            $recursiveIterator = new \RecursiveIteratorIterator($dirIterator, \RecursiveIteratorIterator::CHILD_FIRST);
            foreach ($recursiveIterator as $file) {
                $file->isDir() ? rmdir($file) : unlink($file);
            }
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
                    if (!is_file($filePath) && !is_dir($filePath)) {
                        return false;
                    }

                    return strpos($fileName, IdGenerator::generateOutputTableId(getenv('KBC_CONFIGID'), $query)) !== false;
                }
            )
        );

        $this->assertCount($expectedFiles, $files);

        if ($expectedFiles < 1) {
            return;
        }

        $manifestValidated = false;
        $csvValidated = false;

        foreach ($files as $file) {
            // manifest validation
            if (preg_match('/.manifest$/ui', $file)) {
                $params = Yaml::parse(file_get_contents($file));

                $this->assertArrayHasKey('destination', $params);
                $this->assertArrayHasKey('incremental', $params);
                $this->assertArrayHasKey('primary_key', $params);
                $this->assertArrayHasKey('columns', $params);

                $this->assertTrue($params['incremental']);
                $this->assertEquals($query['primaryKey'], $params['primary_key']);
                $this->assertNotEmpty($params['columns']);

                $this->assertEquals(IdGenerator::generateOutputTableId(getenv('KBC_CONFIGID'), $query), $params['destination']);

                if (isset($query['outputTable'])) {
                    $this->assertEquals($query['outputTable'], $params['destination']);
                }

                $manifestValidated = true;
            }

            // archive validation in slice folder
            if (preg_match('/.csv.gz$/ui', $file) && is_dir($file)) {
                $csvValidatedCount = 0;
                $slicesDirPath = $file;
                $slicedFiles = array_map(
                    function ($fileName) use ($slicesDirPath) {
                        return $slicesDirPath . '/' . $fileName;
                    },
                    array_filter(
                        scandir($file),
                        function ($fileName) use ($slicesDirPath, $query) {
                            $filePath = $slicesDirPath . '/' . $fileName;
                            if (!is_file($filePath)) {
                                return false;
                            }

                            return true;
                        }
                    )
                );

                foreach ($slicedFiles as $slicedFile) {
                    exec("gunzip -d " . escapeshellarg($slicedFile), $output, $return);
                    $this->assertEquals(0, $return);

                    $csvValidatedCount += 1;
                }


                $this->assertGreaterThan(0, count($slicedFiles));
                $this->assertEquals(count($slicedFiles), $csvValidatedCount);
                $csvValidated = true;
            }
        }

        $this->assertTrue($manifestValidated);
        $this->assertTrue($csvValidated);
    }

    private function validateCleanup($query, $project)
    {
        $files = $this->client->listCloudStorageFiles(getenv('KBC_CONFIGID'), $query, $project);
        $this->assertCount(0, $files);
        return true;
    }
}
