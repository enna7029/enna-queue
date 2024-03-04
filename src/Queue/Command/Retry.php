<?php

namespace Enna\Queue\Queue\Command;

use Enna\Framework\Console\Command;
use Enna\Framework\Console\Input\Argument;
use Enna\Framework\Helper\Arr;

class Retry extends Command
{
    protected function configure()
    {
        $this->setName('queue:retry')
            ->addArgument('id', Argument::IS_ARRAY | Argument::REQUIRED, 'The ID of the failed job or "all" to retry all jobs')
            ->setDescription('Retry a failed queue job');
    }

    public function handle()
    {
        foreach ($this->getJobIds() as $id) {
            $job = $this->app['queue.failter']->find($id);

            if (is_null($job)) {
                $this->output->error("Unable to find failed job with ID [{$id}].");
            } else {
                $this->retryJob($job);

                $this->output->info("The failed job [{$id}] has been pushed back onto the queue!");

                $this->app['queue.failer']->forget($id);
            }
        }
    }

    /**
     * Note: 获取要重试的任务id
     * Date: 2024-02-23
     * Time: 15:34
     * @return array
     */
    protected function getJobIds()
    {
        $ids = (array)$this->input->getArgument('id');

        if (count($ids) === 1 && $ids[0] === 'all') {
            $ids = Arr::pluck($this->app['queue.failer']->all(), 'id');
        }

        return $ids;
    }

    /**
     * Note: 重试任务
     * Date: 2024-02-23
     * Time: 16:17
     * @param array $job 任务信息
     * @return void
     */
    protected function retryJob($job)
    {
        $this->app['queue.failer']->connection($job['connection'])->pushRaw(
            $this->resetAttempts($job['payload']),
            $job['queue']
        );
    }

    /**
     * Note: 重置负载的attempts
     * Date: 2024-02-23
     * Time: 16:24
     * @param string $payload 负载的数据
     * @return false|string
     */
    protected function resetAttempts($payload)
    {
        $payload = json_decode($payload, true);

        if (isset($payload['attempts'])) {
            $payload['attempts'] = 0;
        }

        return json_encode($payload);
    }
}