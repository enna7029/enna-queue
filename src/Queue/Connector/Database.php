<?php

namespace Enna\Queue\Queue\Connector;

use Enna\Queue\Queue\Connector;
use Enna\Framework\Db;
use Enna\Orm\Contract\ConnectionInterface;
use Enna\Queue\Queue\InteractsWithTime;
use Enna\Framework\Helper\Collection;
use Enna\Queue\Queue\Job\Database as DatabaseJob;
use Enna\Orm\Db\Query;
use Carbon\Carbon;

class Database extends Connector
{
    use InteractsWithTime;

    /**
     * @var ConnectionInterface
     */
    protected $db;

    /**
     * 数据库
     * @var string
     */
    protected $table;

    /**
     * 默认的队列名称
     * @var mixed|string
     */
    protected $default;

    /**
     * 任务的重试间隔时间(或者说是:多少时间后重试)
     * 解释:这个值最好要大于任务的最长时间时间
     * @var int
     */
    protected $retryAfter;

    public function __construct(ConnectionInterface $db, $table, $default = 'default', $retryAfter = 60)
    {
        $this->db = $db;
        $this->table = $table;
        $this->default = $default;
        $this->retryAfter = 60;
    }

    public static function __make(Db $db, $config)
    {
        $connection = $db->connect($config['connection'] ?? null);

        return new self($connection, $config['table'], $config['queue'], $config['retry_after'] ?? 60);
    }

    /**
     * Note: 推送数据到指定的队列
     * Date: 2024-02-23
     * Time: 16:25
     * @param mixed $job 任务类名称和任务实例
     * @param string $data 负载的数据
     * @param string $queue 队列名称
     * @return int|mixed|string
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushToDatabase($queue, $this->createPayload($job, $data));
    }

    /**
     * Note: 推送原始数据到指定的队列中
     * Date: 2024-02-23
     * Time: 16:26
     * @param mixed $payload 负载的数据
     * @param string $queue 队列
     * @param array $options 选项
     * @return int|mixed|string
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        return $this->pushToDatabase($queue, $payload);
    }

    /**
     * Note: 批量加入消息
     * Date: 2024-02-23
     * Time: 16:38
     * @param mixed $jobs
     * @param string $data
     * @param null $queue
     * @return int
     */
    public function bulk($jobs, $data = '', $queue = null)
    {
        $queue = $this->getQueue($queue);

        $availableAt = $this->availableAt();

        $allData = (new Collection((array)$jobs))->map(
            function ($job) use ($queue, $data, $availableAt) {
                return [
                    'queue' => $queue,
                    'attempts' => 0,
                    'reserve_time' => null,
                    'available_time' => $availableAt,
                    'create_time' => $this->currentTime(),
                    'payload' => $this->createPayload($job, $payload),
                ];
            }
        )->all();

        return $this->db->name($this->table)->insertAll($allData);
    }

    /**
     * Note: 延迟推送数据到指定的队列
     * Date: 2024-02-23
     * Time: 16:40
     * @param int $delay
     * @param mixed $job
     * @param string $data
     * @param null $queue
     * @return int|mixed|string
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        return $this->pushToDatabase($queue, $this->createPayload($job, $data), $delay);
    }

    /**
     * Note: 重新发布任务
     * Date: 2024-02-08
     * Time: 10:51
     * @param string $queue 队列名称
     * @param object $job 任务对象
     * @param int $delay 延迟的秒数
     * @return int|string
     */
    public function release($queue, $job, $delay)
    {
        return $this->pushToDatabase($queue, $job->payload, $delay, $job->attempts);
    }

    /**
     * Note: 推送原始数据到数据库
     * Date: 2024-02-07
     * Time: 14:57
     * @param string $queue 队列名称
     * @param string $payload 数据
     * @param int $delay 延迟的秒数
     * @param int $attempts 尝试次数
     * @return int|string
     */
    protected function pushToDatabase($queue, $payload, $delay = 0, $attempts = 0)
    {
        return $this->db->name($this->table)->insertGetId([
            'queue' => $this->getQueue($queue),
            'attempts' => $attempts,
            'reserve_time' => null,
            'available_time' => $this->availableAt($delay),
            'create_time' => $this->currentTime(),
            'payload' => $payload,
        ]);
    }

    /**
     * Note: 获取队列的第一个消息
     * Date: 2024-02-19
     * Time: 14:38
     * @param string|null $queue 队列名称
     * @return mixed
     */
    public function pop($queue = null)
    {
        $queue = $this->getQueue($queue);

        return $this->db->transaction(function () use ($queue) {
            if ($job = $this->getNextAvailableJob($queue)) {

                $job = $this->markJobAsReserved($job);

                return new DatabaseJob($this->app, $this, $job, $this->connection, $queue);
            }
        });
    }

    protected function getNextAvailableJob($queue)
    {
        $job = $this->db
            ->name($this->table)
            ->lock(true)
            ->where('queue', $this->getQueue($queue))
            ->where(function (Query $query) {
                $query->where(function (Query $query) {
                    $query->whereNull('reserve_time')->where('available_time', '<=', $this->currentTime());
                });

                $expiration = Carbon::now()->subSeconds($this->retryAfter)->getTimestamp();
                $query->whereOr(function (Query $query) use ($expiration) {
                    $query->where('reserve_time', '<=', $expiration);
                });
            })
            ->order('id', 'asc')
            ->find();

        return $job ? (object)$job : null;
    }

    /**
     * Note: 标记任务并设置保留时间
     * Date: 2024-03-01
     * Time: 16:29
     * @param stdClass $job 
     * @return mixed
     * @throws \Enna\Framework\Exception
     */
    protected function markJobAsReserved($job)
    {
        $this->db
            ->name($this->table)
            ->where('id', $job->id)
            ->update([
                'reserve_time' => $job->reserve_time = $this->currentTime(),
                'attempts' => ++$job->attempts
            ]);

        return $job;
    }

    public function size($queue = null)
    {
        return $this->db
            ->name($this->table)
            ->where('queue', $this->getQueue($queue))
            ->count();
    }

    /**
     * Note: 得到队列名称
     * Date: 2024-02-07
     * Time: 14:59
     * @param string $queue
     * @return mixed|string
     */
    protected function getQueue($queue)
    {
        return $queue ?: $this->default;
    }

    /**
     * Note: 删除任务
     * Date: 2024-02-08
     * Time: 10:34
     * @param string|id $id 任务id
     * @return void
     */
    public function deleteReserved($id)
    {
        $this->db->transaction(function () use ($id) {
            if ($this->db->name($this->table)->lock(true)->find($id)) {
                $this->db->name($this->table)->where('id', $id)->delete();
            }
        });
    }
}