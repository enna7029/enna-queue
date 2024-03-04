<?php

namespace Enna\Queue\Queue\Connector;

use Exception;
use Throwable;
use Enna\Queue\Queue\Connector;
use Enna\Queue\Queue\Event\JobProcessing;
use Enna\Queue\Queue\Event\JobProcessed;
use Enna\Queue\Queue\Event\JobFailed;
use Enna\Queue\Queue\Job\Sync as SyncJob;

class Sync extends Connector
{
    public function size($queue = null)
    {
        return 0;
    }

    public function push($job, $data = '', $queue = null)
    {
        $queueJob = $this->resolveJob($this->createPayload($job, $data), $queue);

        try {
            $this->triggerEvent(new JobProcessing($this->connection, $job));

            $queueJob->fire();

            $this->triggerEvent(new JobProcessed($this->connection, $job));
        } catch (Exception | Throwable $e) {
            $this->triggerEvent(new JobFailed($this->connection, $job, $e));

            throw $e;
        }

        return 0;
    }

    protected function resolveJob($payload, $queue)
    {
        return new SyncJob($this->app, $payload, $this->connection, $queue);
    }

    protected function triggerEvent($event)
    {
        $this->app->event->trigger($event);
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {

    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        return $this->push($job, $data, $queue);
    }

    public function pop($queue = null)
    {

    }
}