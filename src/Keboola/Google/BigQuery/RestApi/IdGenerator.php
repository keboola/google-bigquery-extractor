<?php
namespace Keboola\Google\BigQuery\RestApi;

class IdGenerator
{
    /**
     * Generate path for exporting table to Cloud Storage files
     *
     * @param $config
     * @param $project
     * @return string
     */
    public static function generateExportPath($account, $config, $project)
    {
        //Convert name to ID stripping non-alfanumeric chars
        $dirName = preg_replace("/[^A-Za-z0-9_\s-]/", "", $account);
        //Clean multiple dashes or whitespaces
        $dirName = preg_replace("/[\s-]+/", " ", $dirName);
        //Convert whitespaces and underscore to dash
        $dirName = preg_replace("/[\s_]/", "_", $dirName);

        return sprintf(
            "%s/runId-%s/%s/%s_*.%s.gz",
            $project['storage'],
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
     * @param $project
     * @return string
     */
    public static function generateExportMask($account, $config, $project)
    {
        $matches = array();
        if (!preg_match('/^gs\:\/\/([^\/]+)(.*)/ui', $project['storage'], $matches)) {
            throw new \InvalidArgumentException("Invalid Cloud Storage Path given");
        }

        //Convert name to ID stripping non-alfanumeric chars
        $dirName = preg_replace("/[^A-Za-z0-9_\s-]/", "", $account);
        //Clean multiple dashes or whitespaces
        $dirName = preg_replace("/[\s-]+/", " ", $dirName);
        //Convert whitespaces and underscore to dash
        $dirName = preg_replace("/[\s_]/", "_", $dirName);

        return ltrim(
            sprintf(
                "%s/runId-%s/%s/%s_",
                trim($matches[2], '/'),
                getenv('KBC_RUNID'),
                $dirName,
                IdGenerator::generateFileName($account, $config)
            ),
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

    public static function generateOutputTableId($account, $config)
    {
        if (isset($config['outputTable'])) {
            return $config['outputTable'];
        } else {
            return 'in.c-keboola-ex-bigquery.' . IdGenerator::generateFileName($account, $config);
        }
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

    public static function genereateExtractorDataset($location): string
    {
        return sprintf('kbc_extractor_%s', mb_strtolower(str_replace('-', '_', $location)));
    }
}
