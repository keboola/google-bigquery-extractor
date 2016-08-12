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

			$objectResponse = $this->request($objectUrl, 'DELETE', array(), array());

			$statusCode = $objectResponse->getStatusCode();
			return ($statusCode == 204);
		} catch (ClientException $e) {
			throw $e;
		}
	}

	/**
	 * @param $account
	 * @param $config
	 * @return array
	 * @throws \Keboola\Google\ClientBundle\Exception\RestApiException
	 */
	public function listCloudStorageFiles($account, $config)
	{
		$matches = array();
		if (!preg_match('/^gs\:\/\/([^\/]+)(.*)/ui', $config['storage'], $matches)) {
			throw new \InvalidArgumentException("Invalid Cloud Storage Path given");
		}

		$bucket = $matches[1];
		$path = ltrim($matches[1], '/');

		$url = 'https://www.googleapis.com/storage/v1/b/%s/o';
		$url = sprintf($url, $bucket);

		$params = array(
			'fields' => 'items(mediaLink,id,name,bucket),nextPageToken,prefixes',
			'prefix' => IdGenerator::generateExportMask($account, $config)
		);

		$return = array();
		$pageToken = null;

		try {
			while($pageToken !== false) {
				$params['maxResults'] = Client::PAGING;
				$params['pageToken'] = $pageToken;

				$response = $this->request($url, 'GET', array(), $params);

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