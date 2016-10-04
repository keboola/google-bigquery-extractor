<?php
namespace Keboola\Google\BigQuery\Configuration;

use Keboola\Google\BigQuery\Exception\UserException;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;
use Symfony\Component\Yaml\Yaml;

class DefinitionTest extends \PHPUnit_Framework_TestCase
{
	/**
	 * Data provider of valid configs files
	 * @return array
	 */
	public function validConfigsData()
	{
		$return = array();
		$path = __DIR__ .'/../../../../data/config/valid';

		$files = scandir($path);
		foreach ($files AS $file) {
			if (preg_match('/\.yml$/ui', $file)) {
				$return[]  = [$path . "/" . $file, $path . "/" . $file . ".json"] ;
			}
		}

		return $return;
	}

	/**
	 * Data provider of invalid configs files
	 * @return array
	 */
	public function invalidParamsConfigsData()
	{
		$return = array();
		$path = __DIR__ .'/../../../../data/config/invalid/parameters';

		$files = scandir($path);
		foreach ($files AS $file) {
			if (preg_match('/\.yml$/ui', $file)) {
				$return[]  = [$path . "/" . $file] ;
			}
		}

		return $return;
	}

	/**
	 * Data provider of invalid configs files
	 * @return array
	 */
	public function invalidAuthConfigsData()
	{
		$return = array();
		$path = __DIR__ .'/../../../../data/config/invalid/authorization';

		$files = scandir($path);
		foreach ($files AS $file) {
			if (preg_match('/\.yml$/ui', $file)) {
				$return[]  = [$path . "/" . $file] ;
			}
		}

		return $return;
	}

	/**
	 * Test valid configs
	 *
	 * @dataProvider validConfigsData
	 */
	public function testValidConfigs($filePath, $jsonFilePath)
	{
		$parsedParams = [];
		$params = Yaml::parse(file_get_contents($filePath));

		$jsonParams = json_decode(file_get_contents($jsonFilePath), true);

		if (!isset($params['authorization'])) {
			$this->fail(UserException::ERR_MISSING_OAUTH_CONFIG);
		}

		if (!isset($params['parameters'])) {
			$this->fail(UserException::ERR_MISSING_PARAMS_CONFIG);
		}

		// yaml - oauth validation
		$processor = new Processor();
		$processedParameters = $processor->processConfiguration(
			new AuthorizationDefinition(),
			[$params['authorization']]
		);

		// yaml - tokens
		$token = json_decode($processedParameters['oauth_api']['credentials']['#data'], true);
		if (!isset($token['access_token']) || !isset($token['refresh_token'])) {
			$this->fail('Missing access or refresh token data');
		}

		$parsedParams['authorization'] = $processedParameters;

		// params validation
		$processor = new Processor();
		$parsedParams['parameters'] = $processor->processConfiguration(
			new ParamsDefinition(!empty($params['action']) ? $params['action'] : 'run'),
			[$params['parameters']]
		);

		// expected output
		foreach ($jsonParams AS $key => $value) {
			$this->assertArrayHasKey($key, $parsedParams);
			$this->assertEquals($jsonParams[$key], $parsedParams[$key]);
		}
	}

	/**
	 * Test invalid parameters configs
	 *
	 * @dataProvider invalidParamsConfigsData
	 */
	public function testInvalidParamsConfigs($filePath)
	{
		$params = Yaml::parse(file_get_contents($filePath));

		// yaml - params validation
		try {
			if (!isset($params['parameters'])) {
				throw new InvalidConfigurationException(UserException::ERR_MISSING_PARAMS_CONFIG);
			}

			$processor = new Processor();
			$processor->processConfiguration(
				new ParamsDefinition(!empty($params['action']) ? $params['action'] : 'run'),
				[$params['parameters']]
			);

			$this->fail("Validation should produce error");
		} catch (InvalidConfigurationException $e) {

		}
	}

	/**
	 * Test invalid authorzization configs
	 *
	 * @dataProvider invalidAuthConfigsData
	 */
	public function testInvalidAuthConfigs($filePath)
	{
		$params = Yaml::parse(file_get_contents($filePath));

		try {
			if (!isset($params['authorization'])) {
				throw new InvalidConfigurationException(UserException::ERR_MISSING_OAUTH_CONFIG);
			}

			$processor = new Processor();
			$processedParameters = $processor->processConfiguration(
				new AuthorizationDefinition(),
				[$params['authorization']]
			);

			// yaml - tokens
			$token = json_decode($processedParameters['oauth_api']['credentials']['#data'], true);
			if (!isset($token['access_token']) || !isset($token['refresh_token'])) {
				throw new InvalidConfigurationException('Missing access or refresh token data');
			}

			$this->fail("Validation should produce error");
		} catch (InvalidConfigurationException $e) {

		}
	}
}