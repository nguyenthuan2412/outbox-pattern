<?php

declare(strict_types=1);

namespace Outbox\Contracts;

use Outbox\OutboxMessage;

interface RetryPolicy
{
    /**
     * Decide what to do after a failed publish.
     *
     * @return \DateTimeImmutable|null when to retry, or null to give up (dead letter)
     */
    public function nextAttemptAt(OutboxMessage $message, \Throwable $error): ?\DateTimeImmutable;
}
