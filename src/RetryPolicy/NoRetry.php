<?php

declare(strict_types=1);

namespace Outbox\RetryPolicy;

use Outbox\Contracts\RetryPolicy;
use Outbox\OutboxMessage;

final class NoRetry implements RetryPolicy
{
    public function nextAttemptAt(OutboxMessage $message, \Throwable $error): ?\DateTimeImmutable
    {
        return null;
    }
}
