<?php

declare(strict_types=1);

namespace Outbox\RetryPolicy;

use Outbox\Contracts\RetryPolicy;
use Outbox\Exception\NonRetryableException;
use Outbox\OutboxMessage;

/**
 * delay = min(maxDelay, baseDelay * multiplier^(attempts-1)) ± jitter
 */
final class ExponentialBackoff implements RetryPolicy
{
    public function __construct(
        private readonly int $maxAttempts = 10,
        private readonly float $baseDelaySeconds = 1.0,
        private readonly float $multiplier = 2.0,
        private readonly float $maxDelaySeconds = 3600.0,
        private readonly float $jitter = 0.2,
    ) {
        if ($maxAttempts < 1) {
            throw new \InvalidArgumentException('maxAttempts must be >= 1');
        }
    }

    public function nextAttemptAt(OutboxMessage $message, \Throwable $error): ?\DateTimeImmutable
    {
        if ($error instanceof NonRetryableException || $message->attempts >= $this->maxAttempts) {
            return null;
        }

        return new \DateTimeImmutable('@' . sprintf('%.6F', microtime(true) + $this->delayFor($message->attempts)));
    }

    public function delayFor(int $attempts): float
    {
        $delay = $this->baseDelaySeconds * ($this->multiplier ** max(0, $attempts - 1));
        $delay = min($this->maxDelaySeconds, $delay);
        if ($this->jitter > 0) {
            $delay *= 1 + $this->jitter * (mt_rand() / mt_getrandmax() * 2 - 1);
        }

        return max(0.0, $delay);
    }
}
