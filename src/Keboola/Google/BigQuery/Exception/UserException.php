<?php
namespace Keboola\Google\BigQuery\Exception;

class UserException extends \InvalidArgumentException
{
	const ERR_DATA_PARAM = 'Data folder not set.';
	const ERR_MISSING_CONFIG = 'Missing configuration file.';

	const ERR_MISSING_OAUTH_CONFIG = 'Auhorization parameters are missing, contact support please.';
	const ERR_OAUTH_CONFIG = 'Auhorization parameters error, contact support please.';

	const ERR_MISSING_PARAMS_CONFIG = 'Missing parameters configuration.';

}