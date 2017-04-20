<?php
namespace Keboola\Google\BigQuery\Configuration;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class ParamsDefinition implements ConfigurationInterface
{
    private $action;

    public function __construct($action = 'run')
    {
        $this->action = (string) $action;
    }

    /**
     * Generates the configuration tree builder.
     *
     * @return \Symfony\Component\Config\Definition\Builder\TreeBuilder The tree builder
     */
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('parameters');

        // google params
        $google = $rootNode->children()->arrayNode('google');
        if ($this->action !== 'listProjects') {
            $google->isRequired();
        }

        // billable bigquery project
        $param = $google->children()->scalarNode('projectId');
        if ($this->action !== 'listProjects') {
            $param->isRequired()->cannotBeEmpty();
        }

        // cloud storage bucket
        $param = $google->children()->scalarNode('storage');
        if ($this->action !== 'listBuckets' && $this->action !== 'listProjects') {
            $param->isRequired()->cannotBeEmpty();
        }

        // queries
        $rootNode
            ->children()
                ->arrayNode('queries')
                    ->prototype('array')
                        ->children()
                            ->integerNode('id')
                                ->min(0)
                                ->end()
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
                            ->booleanNode('enabled')
                                ->defaultValue(true)
                                ->end()
                            ->booleanNode('useLegacySql')
                                ->defaultValue(true)
                                ->end()
                        ->end()
                    ->end()
            ->end()
        ;

        return $treeBuilder;
    }
}
