<?php

declare(strict_types=1);

namespace Outbox\Swoole;

use Outbox\Contracts\Sleeper;

/**
 * Non-blocking sleep: yields the coroutine instead of blocking the whole worker process.
 */
final class CoroutineSleeper implements Sleeper
{
    public function sleep(float $seconds): void
    {
        if ($seconds <= 0) {
            return;
        }
        Runtime::inCoroutine() ? Runtime::sleep($seconds) : usleep((int) ($seconds * 1_000_000));
    }
}
