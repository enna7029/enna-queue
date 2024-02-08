<?php

namespace Enna\Queue;

use Enna\Framework\Manager;

class Queue extends Manager
{
    protected $namespace = '\\Enna\\Queue\\Queue\\Connector\\';

    protected function resolveType(string $name)
    {
        return $this->app->config->get("queue.connections.{$name}.type", 'sync');
    }

    protected function resolveConfig(string $name)
    {
        return $this->app->config->get("queue.connections.{$name}");
    }

    protected function createDriver(string $name)
    {
        /**
         * @var Queue\Connector $driver
         */
        $driver = parent::createDriver($name);

        $driver->setApp($this->app)->setConnection($name);
    }

    protected function connection($name = null)
    {
        return $this->driver($name);
    }

    public function getDefaultDriver()
    {
        return $this->app->config->get('queue.default');
    }
}