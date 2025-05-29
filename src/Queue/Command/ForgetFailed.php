<?php

namespace Enna\Queue\Queue\Command;

use Enna\Framework\Console\Command;
use Enna\Framework\Console\Input\Argument;

class ForgetFailed extends Command
{
    protected function configure()
    {
        $this->setName('queue:forget')
            ->addArgument('id', Argument::REQUIRED, 'The ID of the failed job')
            ->setDescription('Delete a failed queue job');
    }

    public function handle()
    {
        if ($this->app->get('queue.failer')->forget($this->input->getArgument('id'))) {
            $this->output->info('Failed job deleted successfully!');
        } else {
            $this->output->error('No failed job matches the given ID.');
        }
    }
}