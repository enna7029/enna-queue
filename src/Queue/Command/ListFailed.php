<?php

namespace Enna\Queue\Queue\Command;

use Enna\Framework\Console\Command;
use Enna\Framework\Helper\Collection;
use Enna\Framework\Console\Table;

class ListFailed extends Command
{
    protected $headers = ['ID', 'Connection', 'Queue', 'Class', 'Fail Time'];

    protected function configure()
    {
        $this->setName('queue:failed')
            ->setDescription('List all of the failed queue jobs');
    }

    public function handle()
    {
        if (count($jobs = $this->getFailedJobs()) === 0) {
            $this->output->info('No failed jobs!');
            return;
        }

        $this->displayFailedJobs($jobs);
    }

    protected function displayFailedJobs(array $jobs)
    {
        $table = new Table();

        $table->setHeader($this->headers);
        $table->setRows($jobs);

        $content = $table->render();
        $this->output->writeln($content);
    }

    protected function getFailedJobs(array $jobs)
    {
        $failed = $this->app['queue.failer']->all();

        return (new Collection($failed))->map(function ($failed) {
            return $this->parseFailedJob((array)$failed);
        })->filter()->all();
    }

    protected function parseFailedJob(array $failed)
    {
        $row = array_values($failed);

        array_splice($row, 3, 0, $this->extractJobName($failed['payload']));

        return $row;
    }

    private function extractJobName($payload)
    {
        $payload = json_decode($payload, true);

        if ($payload && (!isset($payload['data']['command']))) {
            return $payload['job'] ?? null;
        } elseif ($payload && isset($payload['data']['command'])) {
            return $this->matchJobName($payload);
        }
    }

    protected function matchJobName($payload)
    {
        preg_match('/"([^"]+)"/', $payload['data']['command'], $matches);

        if (isset($matches[1])) {
            return $matches[1];
        }

        return $payload['job'] ?? null;
    }
}