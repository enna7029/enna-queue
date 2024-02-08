<?php

use Enna\Queue\Facade\Queue;

if (!function_exists('queue')) {
    /**
     * Note: 加入到队列
     * Date: 2024-02-07
     * Time: 9:56
     * @param mixed $job 任务名类
     * @param string $data 参数
     * @param int $delay 延迟的秒
     * @param string $queue 队列名
     */
    function queue($job, $data = '', $delay = 0, $queue = null)
    {
        if ($delay > 0) {
            Queue::later($delay, $job, $data, $queue);
        } else {
            Queue::push($job, $data, $queue);
        }
    }
}