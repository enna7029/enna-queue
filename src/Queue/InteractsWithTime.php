<?php

namespace Enna\Queue\Queue;

use DateTimeInterface;
use DateInterval;
use Carbon\Carbon;

trait InteractsWithTime
{
    /**
     * Note: 得到unix timestamp
     * Date: 2024-02-07
     * Time: 17:01
     * @param int $delay
     * @return int
     */
    protected function availableAt($delay = 0)
    {
        $delay = $this->parseDateInterval($delay);

        return $delay instanceof DateTimeInterface ? $delay->getTimestamp() : Carbon::now()->addRealSeconds($delay)->getTimestamp();
    }

    /**
     * Note: 如果给的值属于interval实例,则转换为datetime实例
     * Date: 2024-02-07
     * Time: 16:58
     * @param DateTimeInterface|DateInterval|int $delay
     * @return DateTimeInterface|int
     */
    protected function parseDateInterval($delay)
    {
        if ($delay instanceof DateInterval) {
            $delay = Carbon::now()->add($delay);
        }

        return $delay;
    }

    protected function currentTime()
    {
        return Carbon::now()->getTimestamp();
    }
}