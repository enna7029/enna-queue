<?php

namespace Enna\Queue\Queue\Job;

use Enna\Framework\App;
use Enna\Queue\Queue\Job;

class Sync extends Job
{
    /**
     * 任务数据
     * @var string
     */
    protected $job;

    public function __construct(App $app, $job, $connection, $queue)
    {
        $this->app = $app;
        $this->job = $job;
        $this->connection = $connection;
        $this->queue = $queue;
    }

    public function attempts()
    {
        return 1;
    }

    public function getRawBody()
    {
        return $this->job;
    }

    public function getJobId()
    {
        return '';
    }

    public function getQueue()
    {
        return 'sync';
    }
}