<?php

namespace Enna\Queue\Queue\Event;

use Enna\Queue\Queue\Job;

class JobFailed
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

    /**
     * 异常对象
     * @var \Exception
     */
    public $exception;

    public function __construct($connection, $job, $exception)
    {
        $this->connection = $connection;
        $this->job = $job;
        $this->exception = $exception;
    }
}