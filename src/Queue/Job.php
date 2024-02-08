<?php

namespace Enna\Queue\Queue;

use Enna\Framework\App;
use Enna\Orm\Contract\ConnectionInterface;
use Enna\Framework\Helper\Arr;
use Enna\Framework\Helper\Str;

abstract class Job
{
    /**
     * @var App
     */
    protected $app;

    /**
     * 任务所属的连接实例
     * @var ConnectionInterface|null
     */
    protected $connection;

    /**
     * 任务所属的队列
     * @var string|null
     */
    protected $queue;

    /**
     * 定义任务是否删除
     * @var bool
     */
    protected $deleted = false;

    /**
     * 定义任务是否重新发布
     * @var bool
     */
    protected $released = false;

    /**
     * 任务处理实例
     * @var object
     */
    private $instance;

    /**
     * 载荷的数据
     * @var array
     */
    private $payload;

    abstract public function attempts();

    abstract public function getRawBody();

    abstract public function getJobId();

    /**
     * Note: 执行任务
     * Date: 2024-02-08
     * Time: 14:16
     */
    public function fire()
    {
        $instance = $this->getResolvedJob();

        [, $method] = $this->getParsedJob();

        $instance->{$method}($this, $this->payload('data'));
    }

    /**
     * Note: 获取任务实例
     * Date: 2024-02-08
     * Time: 14:09
     * @return object|void
     */
    public function getResolvedJob()
    {
        if (empty($this->instance)) {
            [$class] = $this->getParsedJob();

            $this->instance = $this->resolve($class, $this->payload('data'));
        }

        return $this->instance;
    }

    /**
     * Note: 解析任务数据
     * Date: 2024-02-08
     * Time: 14:08
     * @return array|false|string[]
     */
    protected function getParsedJob()
    {
        $job = $this->payload('job');
        $segments = explode('@', $job);

        return count($segments) > 1 ? $segments : [$segments[0], 'fire'];
    }

    /**
     * Note: 解析类
     * Date: 2024-02-08
     * Time: 14:11
     * @param string $name 类型
     * @param array $param
     */
    protected function resolve($name, $param)
    {
        $namespace = $this->app->getNamespace() . '\\job\\';

        $class = strpos($name, '\\') !== false ? $name : $namespace . Str::studly($name);

        return $this->app->make($class, [$param], true);
    }

    /**
     * Note: 获取负载的数据
     * Date: 2024-02-08
     * Time: 14:08
     * @param string|null $name 名称
     * @param mixed|null $default 默认值
     * @return array|mixed
     */
    public function payload($name = null, $default = null)
    {
        if (empty($this->payload)) {
            $this->payload = json_decode($this->getRawBody(), true);
        }
        if (empty($this->name)) {
            return $this->payload;
        }

        return Arr::get($this->payload, $name, $default);
    }

    /**
     * Note: 从队列删除任务
     * Date: 2024-02-08
     * Time: 10:32
     * @return void
     */
    public function delete()
    {
        $this->deleted = true;
    }

    /**
     * Note: 确定任务是否删除
     * Date: 2024-02-08
     * Time: 10:33
     * @return bool
     */
    public function isDeleted()
    {
        return $this->deleted;
    }

    /**
     * Note: 重新发布任务到队列
     * Date: 2024-02-08
     * Time: 10:49
     * @param int $delay
     * @return void
     */
    public function release($delay = 0)
    {
        $this->released = true;
    }

    /**
     * Note: 确定任务对否重新发布到队列
     * Date: 2024-02-08
     * Time: 10:50
     * @return bool
     */
    public function isRelease()
    {
        return $this->released;
    }
}