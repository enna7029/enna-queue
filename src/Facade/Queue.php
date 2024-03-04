<?php

namespace Enna\Queue\Facade;

use Enna\Framework\Facade;

/**
 * Class Queue
 * @package Enna\Queue\Facade
 * @mixin \Enna\Queue\Queue
 */
class Queue extends Facade
{
    protected static function getFacadeClass()
    {
        return 'queue';
    }
}