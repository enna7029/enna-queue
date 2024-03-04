<?php

namespace Enna\Queue\Queue\Job;

use Enna\Framework\App;
use Enna\Queue\Queue\Job;
use Enna\Queue\Queue\Connector\Redis as RedisQueue;

class Redis extends Job
{
    /**
     * @var \Redis
     */
    protected $redis;

    /**
     * @var Object
     */
    protected $job;

    /**
     * @var string
     */
    protected $reserved;

    public function __construct(App $app, RedisQueue $redis, $job, $reserved, $connection, $queue)
    {
        $this->app = $app;
        $this->redis = $redis;
        $this->job = $job;
        $this->reserved = $reserved;
        $this->connection = $connection;
        $this->queue = $queue;
    }

    /**
     * Note: 删除任务
     * Date: 2024-02-29
     * Time: 15:08
     */
    public function delete()
    {
        parent::delete();

        $this->redis->deleteReserved($this->queue, $this);
    }

    /**
     * Note: 重新发布任务
     * Date: 2024-02-29
     * Time: 15:08
     * @param int $delay
     */
    public function release($delay = 0)
    {
        parent::release($delay);

        $this->redis->deleteAndRelease($this->queue, $this, $delay);
    }

    /**
     * Note: 获取任务执行的次数
     * Date: 2024-02-29
     * Time: 15:03
     * @return array|mixed
     */
    public function attempts()
    {
        return $this->payload('attempts') + 1;
    }

    /**
     * Note: 获取任务原始数据
     * Date: 2024-02-29
     * Time: 15:03
     * @return Object
     */
    public function getRawBody()
    {
        return $this->job;
    }

    /**
     * Note: 获取任务标识符
     * Date: 2024-02-29
     * Time: 15:03
     * @return array|mixed
     */
    public function getJobId()
    {
        return $this->payload('id');
    }

    /**
     * Note: 获取潜在保留的任务
     * Date: 2024-02-29
     * Time: 15:05
     * @return string
     */
    public function getReservedJob()
    {
        return $this->reserved;
    }
}