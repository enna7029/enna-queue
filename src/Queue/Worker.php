<?php

namespace Enna\Queue\Queue;

use Enna\Queue\Queue;
use Enna\Framework\Event;
use Enna\Framework\Exception\Handle;
use Enna\Framework\Cache;
use Exception;
use Throwable;
use Enna\Queue\Queue\Event\WorkerStopping;
use Enna\Queue\Queue\Event\JobProcessing;
use Enna\Queue\Queue\Event\JobProcessed;
use Enna\Queue\Queue\Event\JobFailed;
use Enna\Queue\Queue\Event\JobExceptionOccurred;
use Carbon\Carbon;
use Enna\Queue\Queue\Exception\MaxAttemptsExceededException;
use RuntimeException;

class Worker
{
    /**
     * 队列服务
     * @var Queue
     */
    protected $queue;

    /**
     * 事件服务
     * @var Event
     */
    protected $event;

    /**
     * 异常处理服务
     * @var Handle
     */
    protected $handle;

    /**
     * 缓存服务
     * @var Cache|null
     */
    protected $cache;

    /**
     * worker是否终止
     * @var bool
     */
    public $shouldQuid = false;

    /**
     * workder是否暂停
     * @var bool
     */
    public $paused = false;

    public function __construct(Queue $queue, Event $event, Handle $handle, Cache $cache = null)
    {
        $this->queue = $queue;
        $this->event = $event;
        $this->handle = $handle;
        $this->cache = $cache;
    }

    /**
     * Note:
     * Date: 2024-02-19
     * Time: 14:01
     * @param string $connection 驱动名
     * @param string $queue 队列名
     * @param int $delay 延迟时间
     * @param int $sleep 睡眠时间
     * @param int $maxTries 最大尝试次数
     * @param int $memory 内存大小
     * @param int $timeout 超时时间
     */
    public function daemon($connection, $queue, $delay = 0, $sleep = 3, $maxTries = 0, $memory = 128, $timeout = 60)
    {
        if ($this->supportsAsyncSignals()) {
            $this->listenForSignals();
        }

        $lastRestart = $this->getTimestampOfLastQueueRestart();

        while (true) {

            $job = $this->getNextJob($this->queue->connection($connection), $queue);

            if ($this->supportsAsyncSignals()) {
                $this->registerTimeoutHandler($job, $timeout);
            }

            if ($job) {
                $this->runJob($job, $connection, $maxTries, $delay);
            } else {
                $this->sleep($sleep);
            }

            $this->stopIfNecessary($job, $lastRestart, $memory);
        }
    }

    /**
     * Note: 执行下一个任务
     * Date: 2024-02-20
     * Time: 18:27
     * @param string $connection 连接驱动名称
     * @param string $queue 队列名称
     * @param int $delay 延迟秒数
     * @param int $sleep 睡眠秒数
     * @param int $maxTries 最大执行次数
     * @return void
     */
    public function runNextJob($connection, $queue, $delay = 0, $sleep = 3, $maxTries = 0)
    {
        $this->getNextJob($this->queue->connection($connection), $queue);

        if ($job) {
            $this->runJob($job, $connection, $maxTries, $delay);
        } else {
            $this->sleep($sleep);
        }
    }

    /**
     * Note: 获取下一个任务
     * Date: 2024-02-19
     * Time: 14:31
     * @param Connector $connector 驱动
     * @param string $queue 队列名称
     * @return Job
     */
    protected function getNextJob($connector, $queue)
    {
        try {
            foreach (explode(',', $queue) as $queue) {
                if (!is_null($job = $connector->pop($queue))) {
                    return $job;
                }
            }
        } catch (Exception | Throwable $e) {
            $this->handle->report($e);
            $this->sleep(1);
        }
    }

    /**
     * Note: 执行任务
     * Date: 2024-02-19
     * Time: 15:06
     * @param Job $job 任务对象
     * @param string $connection 连接驱动名
     * @param int $maxTries 最大连接数量
     * @param int $delay 延迟时间
     * @return void
     */
    public function runJob($job, $connection, $maxTries, $delay)
    {
        try {
            $this->process($connection, $job, $maxTries, $delay);
        } catch (Exception | Throwable $e) {
            $this->handle->report($e);
        }
    }

    /**
     * Note: 从队列中给一个任务到进程
     * Date: 2024-02-20
     * Time: 16:47
     * @param string $connection 连接驱动名称
     * @param Job $job 任务对象
     * @param int $maxTries 最大尝试次数
     * @param int $delay 延迟秒数
     * @return void
     * @throws Throwable
     */
    public function process($connection, $job, $maxTries = 0, $delay = 0)
    {
        try {
            $this->event->trigger(new JobProcessing($connection, $job));

            $this->markJobAsFailedIfAlreadyExceedsMaxAttempts($connection, $job, (int)$maxTries);

            $job->fire();

            $this->event->trigger(new JobProcessed($connection, $job));
        } catch (Exception | Throwable $e) {
            try {
                if (!$job->hasFailed()) {
                    $this->markJobAsFailedIfWillExceedMaxAttempts($connection, $job, (int)$maxTries, $e);
                }

                $this->event->trigger(new JobExceptionOccurred($connection, $job, $e));
            } finally {
                if (!$job->isDeleted() && !$job->isRelease() && !$job->hasFailed()) {
                    $job->release($delay);
                }
            }

            throw $e;
        }
    }

    /**
     * Note: 标记由于超过指定次数或超时而失败的任务消息
     * Date: 2024-02-20
     * Time: 16:59
     * @param string $connection
     * @param Job $job
     * @param int $maxTries
     */
    protected function markJobAsFailedIfAlreadyExceedsMaxAttempts($connection, $job, $maxTries)
    {
        $maxTries = !is_null($job->maxTries()) ? $job->maxTries() : $maxTries;

        $timeoutAt = $job->timeoutAt();

        if ($timeoutAt && Carbon::now()->getTimestamp() <= $timeoutAt) {
            return;
        }

        if (!$timeoutAt && ($maxTries === 0 || $job->attempts() <= $maxTries)) {
            return;
        }

        $this->failJob($connection, $job, $e = new MaxAttemptsExceededException(
            $job->getName() . ' has been attempted too many times or run too long. The job may have previously timed out.'
        ));

        throw $e;
    }

    /**
     * Note: 标记将要超过指定次数或超时而失败的任务消息
     * Date: 2024-02-20
     * Time: 18:20
     * @param string $connection
     * @param Job $job
     * @param int $maxTries
     * @param Exception $e
     */
    protected function markJobAsFailedIfWillExceedMaxAttempts($connection, $job, $maxTries, $e)
    {
        $maxTries = !is_null($job->maxTries()) ? $job->maxTries() : $maxTries;

        if ($job->timeoutAt() && $job->timeoutAt() <= Carbon::now()->getTimestamp()) {
            $this->failJob($connection, $job, $e);
        }

        if ($maxTries > 0 && $job->attempts() >= $maxTries) {
            $this->failJob($connection, $job, $e);
        }
    }

    /**
     * Note: 任务消息失败处理
     * Date: 2024-02-20
     * Time: 17:21
     * @param string $connection 连接驱动名
     * @param Job $job 任务对象
     * @param Exception $e 异常对象
     */
    protected function failJob($connection, $job, $e)
    {
        $job->markAsFailed();

        if ($job->isDeleted()) {
            return;
        }

        try {
            $job->delete();

            $job->failed($e);
        } finally {
            $this->event->trigger(new JobFailed(
                $connection,
                $job,
                $e ?: new RuntimeException('ManuallyFailed')
            ));
        }
    }

    /**
     * Note: 必要是终止
     * Date: 2024-02-19
     * Time: 15:38
     * @param Job $job 任务对象
     * @param string $lastRestart 队列重启时间
     * @param int $memory 内存限制
     * @return void
     */
    protected function stopIfNecessary($job, $lastRestart, $memory)
    {
        if ($this->shouldQuid || $this->queueShouldRestart($lastRestart)) {
            $this->stop();
        } elseif ($this->memoryExceeded($memory)) {
            $this->stop(12);
        }
    }

    /**
     * Note: 队列是否需要重启
     * Date: 2024-02-19
     * Time: 15:41
     * @param int|null $lastRestart
     * @return bool
     */
    protected function queueShouldRestart($lastRestart)
    {
        return $this->getTimestampOfLastQueueRestart() != $lastRestart;
    }

    /**
     * Note: 是否超时内存限制
     * Date: 2024-02-19
     * Time: 15:42
     * @param int $memoryLimit 内存限制
     * @return bool
     */
    public function memoryExceeded($memoryLimit)
    {
        return (memory_get_usage(true) / 1024 / 1024) >= $memoryLimit;
    }

    /**
     * Note: 是否支持异步信号
     * Date: 2024-02-19
     * Time: 14:05
     * @return bool
     */
    protected function supportsAsyncSignals()
    {
        return extension_loaded('pcntl');
    }

    /**
     * Note: 安装信号处理程序
     * Date: 2024-02-19
     * Time: 14:18
     */
    protected function listenForSignals()
    {
        pcntl_async_signals(true);

        pcntl_signal(SIGTERM, function () {
            $this->shouldQuid = true;
        });


        pcntl_signal(SIGUSR2, function () {
            $this->paused = true;
        });

        pcntl_signal(SIGCONT, function () {
            $this->paused = false;
        });
    }

    /**
     * Note: 注册进程超时处理器
     * Date: 2024-02-19
     * Time: 15:00
     * @param Job $job 任务对象
     * @param int $timeout 超时时间
     * @return void
     */
    protected function registerTimeoutHandler($job, $timeout)
    {
        pcntl_signal(SIGALRM, function () {
            $this->kill(1);
        });

        pcntl_alarm(max($this->timeoutForJob($job, $timeout), 0));
    }

    /**
     * Note: 获取队列重启时间
     * Date: 2024-02-19
     * Time: 14:27
     * @return mixed
     */
    protected function getTimestampOfLastQueueRestart()
    {
        if ($this->cache) {
            $this->cache->get('enna:queue:restart');
        }
    }

    /**
     * Note: 获取任务的超时时间
     * Date: 2024-02-19
     * Time: 15:02
     * @param Job $job 任务对象
     * @param int $timeout 超时时间
     * @return mixed
     */
    protected function timeoutForJob($job, $timeout)
    {
        return $job && !is_null($job->timeout()) ? $job->timeout() : $timeout;
    }

    /**
     * Note: 睡眠指定的妙数
     * Date: 2024-02-19
     * Time: 14:43
     * @param int $seconds
     * @return void
     */
    public function sleep($seconds)
    {
        if ($seconds < 1) {
            usleep($seconds * 1000000);
        } else {
            sleep($seconds);
        }
    }

    /**
     * Note: 停止进程
     * Date: 2024-02-19
     * Time: 15:44
     * @param int $status
     * @return void
     */
    public function stop($status)
    {
        $this->event->trigger(new WorkerStopping($status));

        exit($status);
    }

    /**
     * Note: Kill进程
     * Date: 2024-02-19
     * Time: 14:59
     * @param int $status
     * @return void
     */
    public function kill($status = 0)
    {
        $this->event->trigger(new WorkerStopping($status));

        if (extension_loaded('posix')) {
            posix_kill(getmypid(), SIGKILL);
        }

        exit($status);
    }

}