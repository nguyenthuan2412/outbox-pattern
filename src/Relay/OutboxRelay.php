<?php

declare(strict_types=1);

namespace Outbox\Relay;

use Outbox\Contracts\OutboxStore;
use Outbox\Contracts\Publisher;
use Outbox\Contracts\RetryPolicy;
use Outbox\OutboxMessage;
use Outbox\RetryPolicy\ExponentialBackoff;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Read side: claims due messages from the store, publishes them, records the outcome.
 *
 * Delivery is at-least-once. A message can be published more than once if the process
 * dies between publish() and markPublished(), or if publishing takes longer than the
 * lease. Consumers must be idempotent (dedupe on message id).
 */
final class OutboxRelay
{
    private readonly RetryPolicy $retryPolicy;
    private readonly LoggerInterface $logger;
    private readonly string $workerId;

    public function __construct(
        private readonly OutboxStore $store,
        private readonly Publisher $publisher,
        ?RetryPolicy $retryPolicy = null,
        ?LoggerInterface $logger = null,
        private readonly int $batchSize = 100,
        private readonly int $leaseSeconds = 60,
        ?string $workerId = null,
    ) {
        if ($batchSize < 1) {
            throw new \InvalidArgumentException('batchSize must be >= 1');
        }
        $this->retryPolicy = $retryPolicy ?? new ExponentialBackoff();
        $this->logger = $logger ?? new NullLogger();
        $this->workerId = $workerId ?? (gethostname() ?: 'host') . ':' . getmypid();
    }

    public function workerId(): string
    {
        return $this->workerId;
    }

    /**
     * Claim and process one batch sequentially.
     */
    public function runOnce(): BatchResult
    {
        $messages = $this->claim();
        $result = new BatchResult(claimed: count($messages));
        foreach ($messages as $message) {
            $result->add($this->handle($message));
        }

        return $result;
    }

    /**
     * @return list<OutboxMessage>
     */
    public function claim(?int $limit = null): array
    {
        return $this->store->claim($limit ?? $this->batchSize, $this->workerId, $this->leaseSeconds);
    }

    /**
     * Publish a single claimed message and persist the outcome.
     *
     * Publish errors are handled (retry / dead letter). Store errors are NOT swallowed:
     * if we cannot record the outcome the lease simply expires and the message is retried.
     */
    public function handle(OutboxMessage $message): Outcome
    {
        try {
            $this->publisher->publish($message);
        } catch (\Throwable $e) {
            return $this->fail($message, $e);
        }

        $this->store->markPublished($message->id);

        return Outcome::Published;
    }

    private function fail(OutboxMessage $message, \Throwable $e): Outcome
    {
        $error = sprintf('%s: %s', $e::class, $e->getMessage());
        $retryAt = $this->retryPolicy->nextAttemptAt($message, $e);
        $context = ['id' => $message->id, 'event' => $message->eventType, 'attempt' => $message->attempts, 'exception' => $e];

        if ($retryAt === null) {
            $this->store->markDead($message->id, $error);
            $this->logger->error('Outbox message moved to dead letter', $context);

            return Outcome::Dead;
        }

        $this->store->markFailed($message->id, $error, $retryAt);
        $this->logger->warning('Outbox publish failed, will retry', $context + ['retry_at' => $retryAt->format(\DATE_RFC3339_EXTENDED)]);

        return Outcome::Retry;
    }
}
