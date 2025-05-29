<?php

namespace Enna\Queue\Queue\Failed;

use Enna\Framework\Db;
use Enna\Queue\Queue\FailedJob;
use Carbon\Carbon;
use Enna\Framework\Helper\Collection;

class Database extends FailedJob
{
    /**
     * @var Db
     */
    protected $db;

    /**
     * 数据库表名
     * @var string
     */
    protected $table;

    public function __construct(Db $db, $table)
    {
        $this->db = $db;
        $this->table = $table;
    }

    public static function __make(Db $db, $config)
    {
        return new self($db, $config['table']);
    }

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
    public function log($connection, $queue, $payload, $exception)
    {
        $fail_time = Carbon::now()->toDateTimeString();

        $exception = (string)$exception;

        return $this->getTable()->insertGetId(compact(
            'connection',
            'queue',
            'payload',
            'exception',
            'fail_time'
        ));
    }

    /**
     * Note: 获取所有失败消息的列表
     * Date: 2024-02-21
     * Time: 13:57
     * @return array
     */
    public function all()
    {
        $all = $this->getTable()->order('id', 'desc')->select();

        return (new Collection($all))->all();
    }

    /**
     * Note: 得到单个失败的消息
     * Date: 2024-02-21
     * Time: 13:58
     * @param mixed $id
     * @return mixed
     */
    public function find($id)
    {
        return $this->getTable()->find($id);
    }

    /**
     * Note: 在存储中删除单个失败的消息
     * Date: 2024-02-21
     * Time: 13:59
     * @param mixed $id
     * @return bool
     */
    public function forget($id)
    {
        return $this->getTable()->where('id', $id)->delete() > 0;
    }

    /**
     * Note: 在存储中清除所有失败的消息
     * Date: 2024-02-21
     * Time: 14:00
     * @return void
     */
    public function flush()
    {
        $this->getTable()->delete(true);
    }

    protected function getTable()
    {
        return $this->db->name($this->table);
    }
}