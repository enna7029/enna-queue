<?php

namespace Enna\Queue\Queue\Connector;

use Enna\Queue\Queue\Connector;
use Enna\Framework\Helper\Str;

class Redis extends Connector
{
    /**
     * @var \Redis
     */
    protected $redis;

    /**
     * 默认队列名称
     * @var string
     */
    protected $default;

    /**
     * 任务的过期时间
     * @var int|null
     */
    protected $retryAfter = 60;

    /**
     * 任务的最大执行时间
     * @var int|null
     */
    protected $blockFor = null;

    public function __construct($redis, $default = 'default', $retryAfter = 60, $blockFor = null)
    {
        $this->redis = $redis;
        $this->default = $default;
        $this->retryAfter = $retryAfter;
        $this->blockFor = $blockFor;
    }

    public static function __make($config)
    {
        if (!extension_loaded('redis')) {
            throw new \Exception('redis扩展未安装');
        }

        $redis = new class($config) {
            protected $config;
            protected $client;

            public function __construct($config)
            {
                $this->config = $config;
                $this->client = $this->createClient();
            }

            protected function createClient()
            {
                $config = $this->config;
                $func = $config['persistent'] ? 'pconnect' : 'connect';

                $client = new \Redis();
                $client->$func($config['host'], $config['port'], $config['timeout']);

                if ($config['password'] != '') {
                    $client->auth($config['password']);
                }

                if ($config['select'] != 0) {
                    $client->select($config['select']);
                }

                return $client;
            }

            public function __call($name, $argument)
            {
                try {
                    return call_user_func_array([$this->client, $name], $argument);
                } catch (\RedisException $e) {
                    if (Str::contains($e->getMessage(), 'went away')) {
                        $this->client = $this->createClient();
                    }

                    throw $e;
                }
            }
        };

        return new self($redis, $config['queue'], $config['retry_after'] ?? 60, $config['block_for'] ?? null);
    }
}