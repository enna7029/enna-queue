<?php

namespace Enna\Queue\Queue\Command;

use Enna\Framework\Console\Command;
use Enna\Queue\Queue\InteractsWithTime;
use Enna\Framework\Cache;

class Restart extends Command
{
    use InteractsWithTime;

    protected function configure()
    {
        $this->setName('queue:restart')
            ->setDescription('Restart queue workder daemons after their current job');
    }

    public function handle(Cache $cache)
    {
        $cache->set('enna:queue:restart', $this->currentTime());

        $this->output->info('queue restart signal.');
    }
}