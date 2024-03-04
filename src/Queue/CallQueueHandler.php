<?php

namespace Enna\Queue\Queue;

use Enna\Framework\App;

class CallQueueHandler
{
    /**
     * @var App
     */
    protected $app;

    public function __construct(App $app)
    {
        $this->app = $app;
    }

    public function call(Job $job, array $data)
    {
        $command = unserialize($data['command']);

        $this->app->invoke([$command, 'handle']);

        if (!$job->isDeletedOrReleased()) {
            $job->delete();
        }
    }

    public function failed(array $data)
    {
        $command = unserialize($data['command']);

        if (method_exists($command, 'failed')) {
            $command->failed();
        }
    }
}