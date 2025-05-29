<?php

namespace Enna\Queue\Queue\Event;

use Enna\Queue\Queue\Job;

class JobProcessed
{
    /**
     * 连接驱动名称
     * @var string
     */
    public $connection;

    /**
     * 任务对象
     * @var Job
     */
    public $job;

    public function __construct($connection, $job)
    {
        $this->connection = $connection;
        $this->job = $job;
    }
}