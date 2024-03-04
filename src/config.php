<?php

return [
    'default' => 'database',
    'connections' => [
        'sync' => [
            'type' => 'sync',
        ],
        'database' => [
            'type' => 'database',
            'queue' => 'default',
            'table' => 'jobs',
            'connection' => null,
        ],
        'redis' => [
            'type' => 'redis',
            'queue' => 'default',
            'host' => '127.0.0.1',
            'port' => 6379,
            'password' => '',
            'select' => 0,
            'timeout' => 0,
            'persistent' => false,
        ],
    ],
    'failed' => [
        'type' => 'database',
        'table' => 'failed_jobs'
    ],
];