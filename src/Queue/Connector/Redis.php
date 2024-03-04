<?php

namespace Enna\Queue\Queue\Connector;

use Closure;
use Exception;
use RedisException;
use Enna\Queue\Queue\Connector;
use Enna\Framework\Helper\Str;
use Enna\Queue\Queue\InteractsWithTime;
use Enna\Queue\Queue\Job\Redis as RedisJob;

class Redis extends Connector
{
    use InteractsWithTime;

    /**
     * redis实例
     * @var \Redis
     */
    protected $redis;

    /**
     * 默认队列名称
     * @var string
     */
    protected $default;

    /**
     * 任务的重试间隔时间(或者说是:多少时间后重试)
     * 解释:这个值最好要大于任务的最长时间时间
     * @var int|null
     */
    protected $retryAfter = 60;

    /**
     * 获取任务时阻塞时间
     * 不为0时表示:等待指定的秒,并返回任务
     * 为0时表示:无限的等待,直到返回任务
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

    /**
     * Note: 队列的长度
     * Date: 2024-02-28
     * Time: 17:55
     * @param string|null $queue
     * @return bool|int|mixed
     */
    public function size($queue = null)
    {
        $queue = $this->getQueue($queue);

        return $this->redis->lLen($queue) + $this->redis->zCard("{$queue}:delayed") + $this->redis->zCard("{$queue}:reserved");
    }

    /**
     * Note: 推送数据到指定的队列:列表
     * Date: 2024-02-28
     * Time: 17:55
     * @param mixed $job 任务类名称和任务实例
     * @param string $data 负载的数据
     * @param string|null $queue 队列名称
     * @return int|mixed|void
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $data), $queue);
    }

    /**
     * Note: 推送原始数据到指定的队列中
     * Date: 2024-02-28
     * Time: 18:01
     * @param mixed $payload 负载的数据
     * @param string|null $queue 队列
     * @param array $options 选项
     * @return mixed|null
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        if ($this->redis->rpush($this->getQueue($queue), $payload)) {
            return json_decode($payload, true)['id'] ?? null;
        }
    }

    /**
     * Note: 延迟推送数据到指定的队列
     * Date: 2024-02-28
     * Time: 18:07
     * @param int $delay 延迟的描述
     * @param mixed $job 任务类名称和任务实例
     * @param string $data 负载
     * @param string|null $queue 队列名称
     * @return mixed|void
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        return $this->laterRaw($delay, $this->createPayload($job, $data), $queue);
    }

    /**
     * Note: 获取队列的下一个消息
     * Date: 2024-02-28
     * Time: 18:19
     * @param string|null $queue
     * @return mixed|void
     */
    public function pop($queue = null)
    {
        $this->migration($prefixed = $this->getQueue($queue));

        if (empty($nextJob = $this->retrieveNextJob($prefixed))) {
            return;
        }

        [$job, $reserved] = $nextJob;

        if ($reserved) {
            return new RedisJob($this->app, $this, $job, $reserved, $this->connection, $queue);
        }
    }

    /**
     * Note: 获取队列名
     * Date: 2024-02-28
     * Time: 17:45
     * @param string|null $queue
     * @return string
     */
    protected function getQueue($queue)
    {
        $queue = $queue ?: $this->default;

        return "{queues:{$queue}}";
    }

    /**
     * Note: 延迟推送任务到指定的队列:集合
     * Date: 2024-02-28
     * Time: 18:16
     * @param int $delay 延迟的秒数
     * @param string $payload 负载
     * @param string|null $queue 队列的名称
     * @return mixed|null
     */
    protected function laterRaw($delay, $payload, $queue = null)
    {
        if ($this->redis->zadd(
            $this->getQueue($queue) . ':delayed',
            $this->availableAt($delay),
            $payload
        )) {
            return json_decode($payload, true)['id'] ?? null;
        }
    }

    /**
     * Note: 迁移任何延迟或过期的任务到队列中
     * Date: 2024-02-29
     * Time: 10:09
     * @param string $queue 队列名称
     * @return void
     */
    protected function migration($queue)
    {
        $this->migrateExpiredJobs($queue . ':delayed', $queue);

        if (!is_null($this->retryAfter)) {
            $this->migrateExpiredJobs($queue . ':reserved', $queue);
        }
    }

    /**
     * Note: 迁移延迟或过期任务
     * Date: 2024-02-29
     * Time: 10:13
     * @param string $from 被迁移的队列名称
     * @param string $to 迁移到的队列名称
     * @param bool $attempt
     * @return void
     */
    public function migrateExpiredJobs($from, $to, $attempt = true)
    {
        $this->redis->watch($from);

        $jobs = $this->redis->zRangeByScore($from, '-inf', $this->currentTime());

        if (!empty($jobs)) {
            $this->transaction(function () use ($from, $to, $jobs, $attempt) {

                $this->redis->zRemRangeByRank($from, 0, count($jobs) - 1);

                for ($i = 0; $i < count($job); $i += 100) {

                    $values = array_slice($jobs, $i, 100);

                    $this->redis->rPush($to, ...$values);
                }
            });
        }

        $this->redis->unwatch();
    }

    /**
     * Note: redis事务
     * Date: 2024-02-29
     * Time: 11:01
     * @param Closure $closure
     * @return void
     */
    protected function transaction(Closure $closure)
    {
        $this->redis->multi();
        try {
            call_user_func($closure);
            if (!$this->redis->exec()) {
                $this->redis->discard();
            }
        } catch (Exception $e) {
            $this->redis->discard();
        }
    }

    /**
     * Note: 从队列中检索下一个任务
     * Date: 2024-02-29
     * Time: 11:37
     * @param string $queue
     * @return array
     */
    protected function retrieveNextJob($queue)
    {
        if (!is_null($this->blockFor)) {
            return $this->blockingPop($queue);
        }

        $reserved = false;
        $job = $this->redis->lpop($queue);
        if ($job) {
            $payload = json_decode($job, true);
            $payload['attempts']++;
            $reserved = json_encode($payload);
            $this->redis->zAdd($queue . ':reserved', $this->availableAt($this->retryAfter), $reserved);
        }

        return [$job, $reserved];
    }

    /**
     * Note: 阻塞的方式(指定的秒数)直到检索到下一个任务
     * Date: 2024-02-29
     * Time: 14:17
     * @param string $queue 队列名称
     * @return arary
     */
    protected function blockingPop($queue)
    {
        $rawBody = $this->redis->blPop($queue, $this->blockFor);

        if (!empty($rawBody)) {
            $payload = json_decode($rawBody[1], true);
            $payload['attempts']++;
            $reserved = json_encode($payload);

            $this->redis->zAdd($queue . ':reserved', $this->availableAt($this->retryAfter), $reserved);

            return [$rawBody[1], $reserved];
        }

        return [null, null];
    }

    /**
     * Note: 删除保留任务
     * Date: 2024-02-29
     * Time: 15:30
     * @param string $queue 队列名称
     * @param RedisJob $job 任务对象
     * @return void
     */
    public function deleteReserved($queue, $job)
    {
        $this->redis->zRem($this->getQueue($queue) . ':reserved', $job->getReservedJob());
    }

    /**
     * Note: 删除保留队列中的任务并重新发布
     * Date: 2024-02-29
     * Time: 15:31
     * @param string $queue 队列名称
     * @param RedisJob $job 任务对象
     * @param int $delay 延迟的秒数
     * @return void
     */
    public function deleteAndRelease($queue, $job, $delay)
    {
        $queue = $this->getQueue($queue);

        $reserved = $job->getResolvedJob();

        $this->redis->zRem($queue . ':reserved', $reserved);

        $this->redis->zAdd($queue . ':delayed', $this->availableAt($delay), $reserved);
    }

    /**
     * Note: 创建负载数据
     * Date: 2024-02-29
     * Time: 15:27
     * @param string|Object $job 任务类
     * @param string $data
     * @return array
     */
    protected function createPayloadArray($job, $data = '')
    {
        return array_merge(parent::createPayloadArray($job, $data), [
            'id' => $this->getRandomId(),
            'attempts' => 0,
        ]);
    }

    /**
     * Note: 获取随机ID
     * Date: 2024-02-29
     * Time: 15:27
     * @return string
     */
    protected function getRandomId()
    {
        return Str::random(32);
    }

}