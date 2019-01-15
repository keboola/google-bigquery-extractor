<?php
namespace Keboola\Google\BigQuery\RestApi;

use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Exception\RequestException;
use Keboola\Google\BigQuery\Exception\UserException;
use Keboola\Google\ClientBundle\Google\RestApi;

class Client extends RestApi
{
    const PAGING = 50;

    public function listTableColumns($projectId, $datasetId, $tableId)
    {
        $tableUrl = 'https://www.googleapis.com/bigquery/v2/projects/%s/datasets/%s/tables/%s';
        $tableUrl = sprintf($tableUrl, $projectId, $datasetId, $tableId);

        $response = $this->request($tableUrl);
        $response = \GuzzleHttp\json_decode($response->getBody(), true);

        return $response['schema']['fields'];
    }

    public function deleteCloudStorageFile(array $fileInfo)
    {
        try {
            $objectUrl = 'https://www.googleapis.com/storage/v1/b/%s/o/%s';
            $objectUrl = sprintf($objectUrl, $fileInfo['bucket'], urlencode($fileInfo['name']));

            $objectResponse = $this->request($objectUrl, 'DELETE', [], []);

            $statusCode = $objectResponse->getStatusCode();
            return ($statusCode == 204);
        } catch (ClientException $e) {
            throw $e;
        }
    }

    public function listBuckets($account)
    {
        $return = array();
        $pageToken = null;

        try {
            while ($pageToken !== false) {
                $params = array(
                    'maxResults' => self::PAGING,
                    'project' => $account['projectId'],
                );

                if ($pageToken) {
                    $params['pageToken'] = $pageToken;
                }

                $url = 'https://www.googleapis.com/storage/v1/b';

                $response = $this->request($url, 'GET', [], ['query' => $params]);
                $response = \GuzzleHttp\json_decode($response->getBody(), true);

                if (!array_key_exists('nextPageToken', $response)) {
                    $pageToken = false;
                } else {
                    $pageToken = $response['nextPageToken'];
                }

                if (!empty($response['items'])) {
                    foreach ($response['items'] as $item) {
                        if (empty($item['kind']) || $item['kind'] !== 'storage#bucket') {
                            continue;
                        }

                        $return[] = array(
                            'id' => $item['id'],
                            'name' => $item['name'],
                        );
                    }
                }
            }
        } catch (RequestException $e) {
            throw $e;
        }

        return $return;
    }

    public function listProjects()
    {
        $return = array();
        $pageToken = null;

        try {
            while ($pageToken !== false) {
                $params = array(
                    'maxResults' => self::PAGING,
                );

                if ($pageToken) {
                    $params['pageToken'] = $pageToken;
                }

                $url = 'https://www.googleapis.com/bigquery/v2/projects';

                $response = $this->request($url, 'GET', [], ['query' => $params]);
                $response = \GuzzleHttp\json_decode($response->getBody(), true);
                if (!array_key_exists('nextPageToken', $response)) {
                    $pageToken = false;
                } else {
                    $pageToken = $response['nextPageToken'];
                }

                if (!empty($response['projects'])) {
                    foreach ($response['projects'] as $project) {
                        $return[] = array(
                            'id' => $project['id'],
                            'name' => $project['friendlyName'],
                        );
                    }
                }
            }
        } catch (RequestException $e) {
            throw $e;
        }

        return $return;
    }

    public function datasetExists(string $projectId, string $datasetId): bool
    {
        try {
            $url = sprintf(
                'https://www.googleapis.com/bigquery/v2/projects/%s/datasets/%s',
                $projectId,
                $datasetId
            );

            $response = $this->request($url, 'GET');

            $responseBody = \GuzzleHttp\json_decode($response->getBody(), true);

            if ($response->getStatusCode() == 200 && !empty($responseBody['selfLink'])) {
                return true;
            }
        } catch (RequestException $e) {
            if ($e->getCode() == 404) {
                return false;
            }

            throw $e;
        }
    }

    public function deleteDataset(string $projectId, string $datasetId): bool
    {
        try {
            $url = sprintf(
                'https://www.googleapis.com/bigquery/v2/projects/%s/datasets/%s',
                $projectId,
                $datasetId
            );

            $response = $this->request($url, 'DELETE');
            return $response->getStatusCode() === 204;
        } catch (RequestException $e) {
            throw $e;
        }
    }

    public function createDataset(string $projectId, string $datasetId, string $description = null): array
    {
        $url = sprintf(
            'https://www.googleapis.com/bigquery/v2/projects/%s/datasets',
            $projectId
        );

        $params = array(
            'description' => $description,
            'id' => $datasetId,
            'datasetReference' => [
                'datasetId' => $datasetId,
            ],
        );

        try {
            $response = $this->request(
                $url,
                'POST',
                [
                    'content-type' => 'application/json',
                ],
                [
                    'json' => $params,
                ]
            );

            $responseBody = \GuzzleHttp\json_decode($response->getBody(), true);

            if ($response->getStatusCode() != 200 || empty($responseBody['selfLink'])) {
                throw new UserException('Could not create query dataset');
            }

            return $responseBody;
        } catch (RequestException $e) {
            throw $e;
        }
    }

    /**
     * @param $account
     * @param $config
     * @param $project
     * @return array
     * @throws \Keboola\Google\ClientBundle\Exception\RestApiException
     */
    public function listCloudStorageFiles($account, $config, $project)
    {
        $matches = array();
        if (!preg_match('/^gs\:\/\/([^\/]+)(.*)/ui', $project['storage'], $matches)) {
            throw new \InvalidArgumentException("Invalid Cloud Storage Path given");
        }

        $bucket = $matches[1];
        $path = ltrim($matches[1], '/');

        $url = 'https://www.googleapis.com/storage/v1/b/%s/o';
        $url = sprintf($url, $bucket);

        $params = array(
            'fields' => 'items(mediaLink,id,name,bucket),nextPageToken,prefixes',
            'prefix' => IdGenerator::generateExportMask($account, $config, $project)
        );

        $return = array();
        $pageToken = null;

        try {
            while ($pageToken !== false) {
                $params['maxResults'] = Client::PAGING;
                $params['pageToken'] = $pageToken;


                $response = $this->request($url, 'GET', [], ['query' => $params]);

                $response = \GuzzleHttp\json_decode($response->getBody(), true);
                if (!array_key_exists('nextPageToken', $response)) {
                    $pageToken = false;
                } else {
                    $pageToken = $response['nextPageToken'];
                }

                if (!empty($response['items'])) {
                    foreach ($response['items'] as $file) {
                        $return[] = $file;
                    }
                }
            }
        } catch (RequestException $e) {
            throw $e;
        }

        return $return;
    }
}
