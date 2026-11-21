<?php

declare(strict_types=1);

namespace Outbox\Contracts;

/**
 * Abstracts waiting so the worker loop runs both in blocking PHP (usleep)
 * and inside Swoole coroutines (Coroutine::sleep) without blocking the reactor.
 */
interface Sleeper
{
    public function sleep(float $seconds): void;
}
