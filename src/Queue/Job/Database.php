<?php

namespace Enna\Queue\Queue\Job;

use Enna\Framework\App;
use Enna\Queue\Queue\Job;
use Enna\Queue\Queue\Connector\Database as DatabaseQueue;

class Database extends Job
{
    /**
     * 任务消息对象
     * @var Object
     */
    protected $job;

    /**
     * 任务消息数据库操作对象
     * @var DatabaseQueue
     */
    protected $database;

    public function __construct(App $app, DatabaseQueue $database, $job, $connection, $queue)
    {
        $this->app = $app;
        $this->database = $database;
        $this->job = $job;
        $this->connection = $connection;
        $this->queue = $queue;
    }

    /**
     * Note: 删除任务
     * Date: 2024-02-08
     * Time: 10:37
     * @return void
     */
    public function delete()
    {
        parent::delete();
        $this->database->deleteReserved($this->job->id);
    }

    /**
     * Note: 重新发布任务
     * Date: 2024-02-08
     * Time: 10:51
     * @param int $delay
     * @return void
     */
    public function release($delay = 0)
    {
        parent::release($delay);

        $this->delete();

        $this->database->release($this->queue, $this->job, $delay);
    }

    /**
     * Note: 获取当前任务的尝试次数
     * Date: 2024-02-08
     * Time: 10:53
     * @return int
     */
    public function attempts()
    {
        return (int)$this->job->attempts;
    }

    /**
     * Note: 得到原始的载荷数据
     * Date: 2024-02-08
     * Time: 10:54
     * @return string
     */
    public function getRawBody()
    {
        return $this->job->payload;
    }

    /**
     * Note: 得到任务id
     * Date: 2024-02-08
     * Time: 10:55
     * @return mixed
     */
    public function getJobId()
    {
        return $this->job->id;
    }
}