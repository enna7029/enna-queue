<?php

namespace Enna\Queue\Queue\Command;

use Enna\Framework\Console\Command;
use Enna\Framework\Helper\Str;

class FailedTable extends Command
{
    protected function configure()
    {
        $this->setName('queue:failed-table')
            ->setDescription('Create a migration for the failed queue jobs database table');
    }

    public function handle()
    {
        if (!$this->app->has('migration.creator')) {
            $this->output->error('Install enna-migration first please');
            return;
        }

        $table = $this->app->config->get('queue.failed.table');

        $className = Str::studly("create_{$table}_table");

        $creator = $this->app->get('migration.creator');

        $path = $creator->create($className);

        $contents = file_get_contents(__DIR__ . '/Stubs/failed_jobs.stub');

        $contents = strtr($contents, [
            'CreateFailedJobsTable' => $className,
            '{{table}}' => $table,
        ]);

        file_put_contents($path, $contents);

        $this->output->info('Migration created successfully!');
    }
}