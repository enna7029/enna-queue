<?php

namespace Enna\Queue\Queue;

use Enna\Framework\Service as FrameworkService;
use Enna\Queue\Queue;
use Enna\Framework\Helper\Arr;
use Enna\Framework\Helper\Str;

class Service extends FrameworkService
{
    public function register(): void
    {
        $this->app->bind('queue', Queue::class);

        $this->app->bind('queue.failer', function () {
            $config = $this->app->config->get('queue:failed', []);

            $type = Arr::pull($config, 'type', 'none');

            $class = strpos($type, '\\') !== false ? $type : '\\Enna\\Queue\\Queue\\Failed\\' . Str::studly($type);

            return $this->app->invokeClass($class, [$config]);
        });
    }

    public function boot(): void
    {
        $this->commands([

        ]);
    }
}