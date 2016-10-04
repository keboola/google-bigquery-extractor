<?php
namespace Keboola\Google\BigQuery\RestApi;


use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Exception\RequestException;
use Keboola\Google\ClientBundle\Google\RestApi;

class Client extends RestApi
{
	const PAGING = 50;

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
			while($pageToken !== false) {
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
					foreach ($response['items'] AS $item) {
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
			while($pageToken !== false) {
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
					foreach ($response['projects'] AS $project) {
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
			while($pageToken !== false) {
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
					foreach ($response['items'] AS $file) {
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