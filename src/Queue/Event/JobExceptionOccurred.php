<?php

namespace Enna\Queue\Queue\Event;

use Enna\Queue\Queue\Job;

class JobExceptionOccurred
{
    /**
     * 连接驱动名称
     * @var string
     */
    public $connectionName;

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

    public function __construct($connectionName, $job, $exception)
    {
        $this->connectionName = $connectionName;
        $this->job = $job;
        $this->exception = $exception;
    }
}