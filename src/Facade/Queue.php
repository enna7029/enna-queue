<?php

namespace Enna\Queue\Facade;

use Enna\Framework\Facade;

class Queue extends Facade
{
    protected static function getFacadeClass()
    {
        return 'queue';
    }
}