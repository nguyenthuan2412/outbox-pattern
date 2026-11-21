<?php

declare(strict_types=1);

namespace Outbox\Worker;

final class WorkerOptions
{
    public function __construct(
        /** Sleep when the outbox is empty. Doubles on each empty poll up to maxIdleSleep. */
        public readonly float $idleSleep = 0.2,
        public readonly float $maxIdleSleep = 2.0,
        /** Sleep after an unexpected error (e.g. DB down) before polling again. */
        public readonly float $errorSleep = 5.0,
        /** Stop when a poll returns nothing (useful for cron / tests). */
        public readonly bool $stopWhenEmpty = false,
        /** Stop after processing this many messages (0 = unlimited). */
        public readonly int $maxMessages = 0,
        /** Stop after this many seconds (0 = unlimited). Let your supervisor restart it. */
        public readonly float $maxRuntime = 0,
        /** Stop when memory usage exceeds this many MB (0 = unlimited). */
        public readonly int $memoryLimitMb = 0,
    ) {
    }
}
