<?php
namespace Keboola\Google\BigQuery\RestApi;


class IdGenerator
{
	/**
	 * Generate path for exporting table to Cloud Storage files
	 *
	 * @param $config
	 * @return string
	 */
	public static function generateExportPath($account, $config)
	{
		//Convert name to ID stripping non-alfanumeric chars
		$dirName = preg_replace("/[^A-Za-z0-9_\s-]/", "", $account);
		//Clean multiple dashes or whitespaces
		$dirName = preg_replace("/[\s-]+/", " ", $dirName);
		//Convert whitespaces and underscore to dash
		$dirName = preg_replace("/[\s_]/", "_", $dirName);

		return sprintf(
			"%s/runId-%s/%s/%s_*.%s.gz",
			$config['storage'],
			getenv('KBC_RUNID'),
			$dirName,
			IdGenerator::generateFileName($account, $config),
			strtolower($config['format'])
		);
	}

	/**
	 * Generate file prefix for searching exported table files
	 *
	 * @param $account
	 * @param $config
	 * @return string
	 */
	public static function generateExportMask($account, $config)
	{
		$matches = array();
		if (!preg_match('/^gs\:\/\/([^\/]+)(.*)/ui', $config['storage'], $matches)) {
			throw new \InvalidArgumentException("Invalid Cloud Storage Path given");
		}

		//Convert name to ID stripping non-alfanumeric chars
		$dirName = preg_replace("/[^A-Za-z0-9_\s-]/", "", $account);
		//Clean multiple dashes or whitespaces
		$dirName = preg_replace("/[\s-]+/", " ", $dirName);
		//Convert whitespaces and underscore to dash
		$dirName = preg_replace("/[\s_]/", "_", $dirName);

		return ltrim(sprintf(
			"%s/runId-%s/%s/%s_", trim($matches[2], '/'),
			getenv('KBC_RUNID'),
			$dirName,
			IdGenerator::generateFileName($account, $config)),
			'/'
		);
	}

	/**
	 * Generate Big Query table name
	 *
	 * @param $account
	 * @param $config
	 * @return string
	 */
	public static function generateTableName($account, $config)
	{
		//Convert name to ID stripping non-alfanumeric chars
		$tableName = preg_replace("/[^A-Za-z0-9_\s-]/", "", sprintf("%s_%s", $account, $config['name']));
		//Clean multiple dashes or whitespaces
		$tableName = preg_replace("/[\s-]+/", " ", $tableName);
		//Convert whitespaces and underscore to dash
		$tableName = preg_replace("/[\s_]/", "_", $tableName);

		return $tableName;
	}

	public static function generateFileName($account, $config)
	{
		//Convert name to ID stripping non-alfanumeric chars
		$fileName = preg_replace("/[^A-Za-z0-9_\s-]/", "", $config['name']);
		//Clean multiple dashes or whitespaces
		$fileName = preg_replace("/[\s-]+/", " ", $fileName);
		//Convert whitespaces and underscore to dash
		$fileName = preg_replace("/[\s_]/", "_", $fileName);

		return $fileName;
	}
}