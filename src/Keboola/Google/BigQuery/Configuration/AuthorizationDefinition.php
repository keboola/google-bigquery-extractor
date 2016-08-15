<?php
namespace Keboola\Google\BigQuery\Configuration;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class AuthorizationDefinition implements ConfigurationInterface
{

	/**
	 * Generates the configuration tree builder.
	 *
	 * @return \Symfony\Component\Config\Definition\Builder\TreeBuilder The tree builder
	 */
	public function getConfigTreeBuilder()
	{
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('authorization');

		// oauth configuration
		$rootNode
			->children()
				->arrayNode('oauth_api')
					->isRequired()
					->children()
						->arrayNode('credentials')
							->isRequired()
							->children()
								->variableNode('id')->end()
								->variableNode('authorizedFor')->end()
								->variableNode('creator')->end()
								->variableNode('created')->end()
								->variableNode('oauthVersion')->end()
								->scalarNode('#data')
									->isRequired()
									->cannotBeEmpty()
									->end()
								->scalarNode('appKey')
									->isRequired()
									->cannotBeEmpty()
									->end()
								->scalarNode('#appSecret')
									->isRequired()
									->cannotBeEmpty()
									->end()
					->end()
			->end()
		;

		return $treeBuilder;
	}
}