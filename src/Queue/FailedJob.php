<?php

namespace Enna\Queue\Queue;

abstract class FailedJob
{
    /**
     * Note: 将失败的消息日志存储起来
     * Date: 2024-02-21
     * Time: 11:55
     * @param string $connection 连接驱动名称
     * @param string $queue 队列名称
     * @param string $payload 负载数据
     * @param \Exception $exception 异常对象
     * @return mixed
     */
    abstract public function log($connection, $queue, $payload, $exception);

    /**
     * Note: 获取所有失败消息的列表
     * Date: 2024-02-21
     * Time: 13:57
     * @return mixed
     */
    abstract public function all();

    /**
     * Note: 得到单个失败的消息
     * Date: 2024-02-21
     * Time: 13:58
     * @param mixed $id
     * @return mixed
     */
    abstract public function find($id);

    /**
     * Note: 在存储中删除单个失败的消息
     * Date: 2024-02-21
     * Time: 13:59
     * @param mixed $id
     * @return mixed
     */
    abstract public function forget($id);

    /**
     * Note: 在存储中清除所有失败的消息
     * Date: 2024-02-21
     * Time: 14:00
     * @return mixed
     */
    abstract public function flush();
}