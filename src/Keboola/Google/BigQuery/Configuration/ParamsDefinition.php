<?php
namespace Keboola\Google\BigQuery\Configuration;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class ParamsDefinition implements ConfigurationInterface
{

	/**
	 * Generates the configuration tree builder.
	 *
	 * @return \Symfony\Component\Config\Definition\Builder\TreeBuilder The tree builder
	 */
	public function getConfigTreeBuilder()
	{
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('parameters');

		// queries
		$rootNode
			->children()
				->arrayNode('google')
					->isRequired()
					->children()
						->scalarNode('projectId')
							->isRequired()
							->cannotBeEmpty()
							->end()
						->scalarNode('storage')
							->isRequired()
							->cannotBeEmpty()
							->end()
						->end()
					->end()
				->arrayNode('queries')
					->prototype('array')
						->children()
							->scalarNode('name')
								->isRequired()
								->cannotBeEmpty()
								->end()
							->scalarNode('query')
								->isRequired()
								->cannotBeEmpty()
								->end()
							->scalarNode('outputTable')
								->end()
							->booleanNode('flattenResults')
								->defaultValue(true)
								->end()
							->booleanNode('incremental')
								->defaultValue(false)
								->end()
							->arrayNode('primaryKey')
								->prototype('scalar')
									->end()
								->end()
						->end()
					->end()
			->end()
		;

		return $treeBuilder;
	}
}