<?php

declare(strict_types=1);

namespace Outbox\Worker;

use Outbox\Contracts\Sleeper;

final class BlockingSleeper implements Sleeper
{
    public function sleep(float $seconds): void
    {
        if ($seconds > 0) {
            usleep((int) ($seconds * 1_000_000));
        }
    }
}
