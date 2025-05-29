<?php

namespace Enna\Queue\Queue\Command;

use Enna\Framework\Console\Command;
use Enna\Framework\Console\Input;
use Enna\Framework\Console\Input\Argument;
use Enna\Framework\Console\Input\Option;
use Enna\Framework\Console\Output;
use Enna\Queue\Queue\Listener;

class Listen extends Command
{
    /**
     * @var Listener
     */
    protected $listener;

    public function __construct(Listener $listener)
    {
        parent::__construct();

        $this->listener = $listener;

        $this->listener->setOutputHandler(function ($type, $line) {
            $this->output->write($line);
        });
    }

    protected function configure()
    {
        $this->setName('queue:listen')
            ->addArgument('connection', Argument::OPTIONAL, 'The name of the queue connection to work', null)
            ->addOption('queue', null, Option::VALUE_OPTIONAL, 'The queue to listen on')
            ->addOption('delay', null, Option::VALUE_OPTIONAL, 'Amount of time to delay failed jobs', 0)
            ->addOption('memory', null, Option::VALUE_OPTIONAL, 'The memory limit in megabytes', 128)
            ->addOption('timeout', null, Option::VALUE_OPTIONAL, 'The number of seconds a child process can run', 60)
            ->addOption('sleep', null, Option::VALUE_OPTIONAL, 'Number of seconds to sleep when no job is available', 3)
            ->addOption('tries', null, Option::VALUE_OPTIONAL, 'Number of times to attempt a job before logging it failed', 0)
            ->setDescription('Listen to a give queue');
    }

    public function execute(Input $input, Output $output)
    {
        $connection = $input->getArgument('connection') ?: $this->app->config->get('queue.default');

        $queue = $input->getOption('queue') ?: $this->app->config->get("queue.connections.{$connection}.queue", 'default');
        $delay = $input->getOption('delay');
        $memory = $input->getOption('memory');
        $timeout = $input->getOption('timeout');
        $sleep = $input->getOption('sleep');
        $tries = $input->getOption('tries');

        $this->listener->listen($connection, $queue, $delay, $sleep, $tries, $memory, $timeout);
    }
}