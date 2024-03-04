<?php

namespace Enna\Queue\Queue\Command;

use Enna\Framework\Console\Command;
use Enna\Framework\Console\Input;
use Enna\Framework\Console\Output;
use Enna\Queue\Queue\Job;
use Enna\Queue\Queue\Worker;
use Enna\Framework\Console\Input\Argument;
use Enna\Framework\Console\Input\Option;
use Enna\Queue\Queue\Event\JobProcessing;
use Enna\Queue\Queue\Event\JobProcessed;
use Enna\Queue\Queue\Event\JobFailed;

class Work extends Command
{
    /**
     * 工作进程对象
     * @var Worker
     */
    protected $worker;

    public function __construct(Worker $worker)
    {
        parent::__construct();
        $this->worker = $worker;
    }

    public function configure()
    {
        $this->setName('queue:work')
            ->addArgument('connection', Argument::OPTIONAL, 'The name of the queue connection to work', null)
            ->addOption('queue', null, Option::VALUE_OPTIONAL, 'The queue to listen on')
            ->addOption('once', null, Option::VALUE_NONE, 'Only process the next job on the queue')
            ->addOption('delay', null, Option::VALUE_OPTIONAL, 'Amount of time to delay failed jobs', 0)
            ->addOption('force', null, Option::VALUE_NONE, 'Force the worker to run even in maintenance mode')
            ->addOption('memory', null, Option::VALUE_OPTIONAL, 'The memory limit in megabytes', 128)
            ->addOption('timeout', null, Option::VALUE_OPTIONAL, 'The number of seconds a child process can run', 60)
            ->addOption('sleep', null, Option::VALUE_OPTIONAL, 'Number of seconds to sleep when no job is available', 3)
            ->addOption('tries', null, Option::VALUE_OPTIONAL, 'Number of times to attempt a job before logging it failed', 0)
            ->setDescription('Process the next job on a queue');
    }

    public function execute(Input $input, Output $output)
    {
        $connection = $input->getArgument('connection') ?: $this->app->config->get('queue.default');

        $queue = $input->getOption('queue') ?: $this->app->config->get("queue.connections.{$connection}.queue", 'default');
        $delay = $input->getOption('delay');
        $sleep = $input->getOption('sleep');
        $tries = $input->getOption('tries');

        $this->listenForEvents();

        if ($input->getOption('once')) {
            $this->worker->runNextJob($connection, $queue, $delay, $sleep, $tries);
        } else {
            $memory = $input->getOption('memory');
            $timeout = $input->getOption('timeout');
            $this->worker->daemon($connection, $queue, $delay, $sleep, $tries, $memory, $timeout);
        }
    }

    /**
     * Note: 注册事件
     * Date: 2024-02-21
     * Time: 11:27
     */
    protected function listenForEvents()
    {
        $this->app->event->listen(JobProcessing::class, function (JobProcessing $event) {
            $this->writeOutput($event->job, 'starting');
        });

        $this->app->event->listen(JobProcessed::class, function (JobProcessed $event) {
            $this->writeOutput($event->job, 'success');
        });

        $this->app->event->listen(JobFailed::class, function (JobFailed $event) {
            $this->writeOutput($event->job, 'failed');

            $this->logFailedJob($event);
        });
    }

    /**
     * Note: 输出任务状态
     * Date: 2024-02-21
     * Time: 11:31
     * @param Job $job
     * @param $status
     */
    protected function writeOutput(Job $job, $status)
    {
        switch ($status) {
            case 'starting':
                $this->writeStatus($job, 'Processing', 'comment');
                break;
            case 'success':
                $this->writeStatus($job, 'Processed', 'info');
                break;
            case 'failed':
                $this->writeStatus($job, 'Failed', 'error');
                break;
        }
    }

    /**
     * Note: 格式化输出的任务状态
     * Date: 2024-02-21
     * Time: 11:40
     * @param Job $job
     * @param $status
     * @param $type
     */
    protected function writeStatus(Job $job, $status, $type)
    {
        $this->output->writeln(sprintf(
            "<{$type}>[%s][%s] %s</{$type}> %s",
            date('Y-m-d H:i:s'),
            $job->getJobId(),
            str_pad("{$status}:", 11),
            $job->getName()
        ));
    }

    /**
     * Note: 记录失败任务
     * Date: 2024-02-21
     * Time: 11:41
     * @param JobFailed $event
     */
    protected function logFailedJob(JobFailed $event)
    {
        $this->app['queue.failer']->log(
            $event->connection,
            $event->job->getQueue(),
            $event->job->getRawBody(),
            $event->exception
        );
    }
}