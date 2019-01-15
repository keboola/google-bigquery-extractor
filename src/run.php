<?php
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Monolog\Handler\NullHandler;
use Monolog\Formatter\LineFormatter;
use Keboola\Google\BigQuery\Exception;
use Keboola\Google\BigQuery\Extractor;
use Symfony\Component\Yaml\Yaml;
use Symfony\Component\Yaml\Exception\ParseException;

require_once(__DIR__ . "/../bootstrap.php");

const APP_NAME = 'ex-google-bigquery';

$logger = new \Monolog\Logger(APP_NAME, array(
    (new StreamHandler('php://stdout', Logger::INFO))->setFormatter(new LineFormatter("%message%\n")),
    (new StreamHandler('php://stderr', Logger::ERROR))->setFormatter(new LineFormatter("%message%\n")),
));

set_error_handler(
    function ($errno, $errstr, $errfile, $errline, array $errcontext) {
        if (0 === error_reporting()) {
            return false;
        }
        throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
    }
);

$action = Extractor::DEFAULT_ACTION;

try {
    // load config
    $arguments = getopt("d::", ["data::"]);
    if (!isset($arguments["data"])) {
        throw new Exception\UserException(Exception\UserException::ERR_DATA_PARAM);
    }

    if (!file_exists($arguments["data"] . "/config.yml")) {
        throw new Exception\UserException(Exception\UserException::ERR_MISSING_CONFIG);
    }

    $config = Yaml::parse(file_get_contents($arguments['data'] . "/config.yml"));
    if (empty($config)) {
        throw new ParseException("Could not parse config file");
    }

    if (isset($config["action"])) {
        $action = $config["action"];
    }

    $extractor = new Extractor([
        'logger' => $logger,
    ]);

    if ($action !== Extractor::DEFAULT_ACTION) {
        $logger->setHandlers(array(new NullHandler(Logger::INFO)));
    }


    $result = $extractor->setConfig($config)->run();
    if ($result !== null) {
        echo json_encode($result);
    }

    $logger->info("Extractor finished successfully.");
    exit(0);
} catch (Exception\UserException $e) {
    $logger->log('error', $e->getMessage(), []);

    if ($action !== Extractor::DEFAULT_ACTION) {
        echo $e->getMessage();
    }

    exit(1);
} catch (\Throwable $e) {
    $logger->critical(
        get_class($e) . ':' . $e->getMessage(),
        [
            'errFile' => $e->getFile(),
            'errLine' => $e->getLine(),
            'errCode' => $e->getCode(),
            'errTrace' => $e->getTraceAsString(),
            'errPrevious' => $e->getPrevious() ? get_class($e->getPrevious()) : '',
        ]
    );
    exit(2);
}
