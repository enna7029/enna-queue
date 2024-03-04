<?php

namespace Enna\Queue\Queue;

use Closure;
use Enna\Framework\App;
use Symfony\Component\Process\Process;
use Symfony\Component\Process\PhpExecutableFinder;

class Listener
{
    /**
     * 输出处理器
     * @var Closure|null
     */
    protected $outoutHandler;

    /**
     * 命令行路径
     * @var string
     */
    protected $commandPath;

    public function __construct($commandPath)
    {
        $this->commandPath = $commandPath;
    }

    public static function __make(App $app)
    {
        return new self($app->getRootPath());
    }

    /**
     * Note: 创建并执行work进程
     * Date: 2024-02-23
     * Time: 14:04
     * @param string $connection
     * @param string $queue
     * @param int $delay
     * @param int $sleep
     * @param int $maxTries
     * @param int $memory
     * @param int $timeout
     */
    public function listen($connection, $queue, $delay = 0, $sleep = 3, $maxTries = 0, $memory = 128, $timeout = 60)
    {
        $process = $this->makeProcess($connection, $queue, $delay, $sleep, $maxTries, $memory, $timeout);

        while (true) {
            $this->runProcess($process, $memory);
        }
    }

    /**
     * Note: 创建进程
     * Date: 2024-02-23
     * Time: 11:50
     * @param string $connection
     * @param string $queue
     * @param int $delay
     * @param int $sleep
     * @param int $maxTries
     * @param int $memory
     * @param int $timeout
     * @return Process
     */
    public function makeProcess($connection, $queue, $delay, $sleep, $maxTries, $memory, $timeout)
    {
        $command = array_filter([
            $this->phpBinary(),
            'think',
            'queue:work',
            $connection,
            '--once',
            "--queue={$queue}",
            "--delay={$delay}",
            "--sleep={$sleep}",
            "--maxTries={$maxTries}",
            "--memory={$memory}",
        ], function ($value) {
            return !is_null($value);
        });

        return new Process($command, $this->commandPath, null, null, $timeout);
    }

    /**
     * Note: 执行进程
     * Date: 2024-02-23
     * Time: 11:51
     * @param Process $process
     * @param int $memory
     */
    public function runProcess(Process $process, $memory)
    {
        $process->run(function ($type, $line) {
            $this->handleWorkerOutput($type, $line);
        });

        if ($this->memoryExceeded($memory)) {
            $this->stop();
        }
    }

    /**
     * Note: 获取PHP二进制文件
     * Date: 2024-02-23
     * Time: 11:44
     * @return false|string
     */
    protected function phpBinary()
    {
        return (new PhpExecutableFinder)->find(false);
    }

    /**
     * Note: 输出信息
     * Date: 2024-02-23
     * Time: 14:47
     * @param int $type
     * @param string $line
     * @return void
     */
    protected function handleWorkerOutput($type, $line)
    {
        if (isset($this->outoutHandler)) {
            call_user_func($this->outoutHandler, $type, $line);
        }
    }

    /**
     * Note: 检查内存是否超限
     * Date: 2024-02-23
     * Time: 14:47
     * @param int $memoryLimit
     * @return bool
     */
    public function memoryExceeded($memoryLimit)
    {
        return (memory_get_usage() / 1024 / 1024) >= $memoryLimit;
    }

    /**
     * Note: 停止进程
     * Date: 2024-02-23
     * Time: 14:48
     * @return void
     */
    public function stop()
    {
        die;
    }

    /**
     * Note: 设置输出处理器
     * Date: 2024-02-23
     * Time: 14:49
     * @param Closure $outputHandler
     * @return void
     */
    public function setOutputHandler(Closure $outputHandler)
    {
        $this->outoutHandler = $outputHandler;
    }
}