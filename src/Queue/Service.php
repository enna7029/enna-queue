<?php

namespace Enna\Queue\Queue;

use Enna\Framework\Service as FrameworkService;
use Enna\Queue\Queue;
use Enna\Framework\Helper\Arr;
use Enna\Framework\Helper\Str;
use Enna\Queue\Queue\Command\FailedTable;
use Enna\Queue\Queue\Command\FlushFailed;
use Enna\Queue\Queue\Command\ForgetFailed;
use Enna\Queue\Queue\Command\Listen;
use Enna\Queue\Queue\Command\ListFailed;
use Enna\Queue\Queue\Command\Restart;
use Enna\Queue\Queue\Command\Retry;
use Enna\Queue\Queue\Command\Table;
use Enna\Queue\Queue\Command\Work;

class Service extends FrameworkService
{
    public function register(): void
    {
        $this->app->bind('queue', Queue::class);

        $this->app->bind('queue.failer', function () {

            $config = $this->app->config->get('queue.failed', []);

            $type = Arr::pull($config, 'type', 'none');

            $class = strpos($type, '\\') !== false ? $type : '\\Enna\\Queue\\Queue\\Failed\\' . Str::studly($type);

            return $this->app->invokeClass($class, [$config]);
        });
    }

    public function boot(): void
    {
        $this->commands([
            FailedTable::class,
            FlushFailed::class,
            ForgetFailed::class,
            Listen::class,
            ListFailed::class,
            Restart::class,
            Retry::class,
            Table::class,
            Work::class,
        ]);
    }
}