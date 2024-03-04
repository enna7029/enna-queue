<?php

namespace Enna\Queue\Queue\Event;

class WorkerStopping
{
    /**
     * 退出的状态
     * @var int
     */
    public $status;

    public function __construct($status)
    {
        $this->status = $status;
    }
}