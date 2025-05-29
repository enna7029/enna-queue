<?php

namespace Enna\Queue\Queue\Failed;

use Enna\Queue\Queue\FailedJob;

class None extends FailedJob
{
    public function log($connection, $queue, $payload, $exception)
    {

    }

    public function all()
    {
        return [];
    }

    public function find($id)
    {

    }

    public function forget($id)
    {
        return true;
    }

    public function flush()
    {
        
    }
}