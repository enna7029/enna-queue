<?php

namespace Enna\Queue\Queue\Command;

use Enna\Framework\Console\Command;
use Enna\Framework\Helper\Str;

class Table extends Command
{
    protected function configure()
    {
        $this->setName('queue:table')
            ->setDescription('Create a migration for the queue jobs database table');
    }

    public function handle()
    {
        if (!$this->app->has('migration.creator')) {
            $this->output->error('Install enna-migration first please');
            return;
        }

        $table = $this->app->config->get('queue.connections.database.table');

        $className = Str::studly("create_{$table}_table");

        $creator = $this->app->get('migration.creator');

        $path = $creator->create($className);

        $contents = file_get_contents(__DIR__ . '/Stubs/jobs.stub');

        $contents = strtr($contents, [
            'CreateJobsTable' => $className,
            '{{table}}' => $table,
        ]);

        file_put_contents($path, $contents);

        $this->output->info('Migration created successfully!');
    }
}