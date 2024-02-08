<?php

namespace Enna\Queue\Queue;

use Enna\Framework\App;

abstract class Connector
{
    /**
     * @var App
     */
    protected $app;

    /**
     * 队列的连接器
     * @var string
     */
    protected $connection;

    /**
     * Note: 队列的长度
     * Date: 2024-02-07
     * Time: 10:33
     * @param string $queue 队列名称
     * @return mixed
     */
    abstract public function size($queue = null);

    /**
     * Note: 推送消息到队列
     * Date: 2024-02-07
     * Time: 10:34
     * @param mixed $job 任务类
     * @param string $data 数据
     * @param string $queue 队列名称
     * @return mixed
     */
    abstract public function push($job, $data = '', $queue = null);

    /**
     * Note: 推送原始消息到队列
     * Date: 2024-02-07
     * Time: 10:35
     * @param mixed $payload 消息
     * @param string $queue 队列名称
     * @param array $options 选项
     * @return mixed
     */
    abstract public function pushRaw($payload, $queue = null, array $options = []);

    /**
     * Note: 延迟推送消息到队列
     * Date: 2024-02-07
     * Time: 10:36
     * @param int $delay 延迟的秒数
     * @param mixed $job 任务类
     * @param string $data 数据
     * @param string $queue 队列名称
     * @return mixed
     */
    abstract public function later($delay, $job, $data = '', $queue = null);

    /**
     * Note: 消息出栈
     * Date: 2024-02-07
     * Time: 10:45
     * @param string $queue 队列名称
     * @return mixed
     */
    abstract public function pop($queue = null);

    public function setApp(App $app)
    {
        $this->app = $app;

        return $this;
    }

    public function setConnection($name)
    {
        $this->connection = $name;

        return $this;
    }

    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * Note: 创建负载
     * Date: 2024-02-07
     * Time: 10:51
     * @param mixed $job 任务类
     * @param string $data 数据
     * @return string
     */
    protected function createPayload($job, $data = '')
    {
        $payload = $this->createPayloadArray($job, $data);

        $payload = json_encode($payload);

        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new \InvalidArgumentException('Unable to create payload:' . json_last_error_msg());
        }

        return $payload;
    }

    protected function createPayloadArray($job, $data = '')
    {
        return is_object($job) ? $this->createObjectPayload($job) : $this->createPlainPayload($job, $data);
    }

    protected function createPlainPayload($job, $data)
    {
        return [
            'job' => $job,
            'maxTries' => null,
            'timeout' => null,
            'data' => $data,
        ];
    }

    protected function createObjectPayload($job)
    {
        return [
            'job' => 'Enna\Queue\Queue\CallQueueHandler@call',
            'maxTries' => $job->tries ?? null,
            'timeout' => $job->timeout ?? null,
            'timeoutAt' => $this->getJobExpiration($job),
            'data' => [
                'commandName' => get_class($job),
                'command' => serialize(clone $job),
            ],
        ];
    }

    protected function getJobExpiration($job)
    {
        if (!method_exists($job, 'retryUntil') && !isset($job->timeoutAt)) {
            return;
        }

        $expiration = $job->timeoutAt ?? $job->retryUntil();

        return $expiration instanceof \DateTimeInterface ? $expiration->getTimestamp() : $expiration;
    }
}