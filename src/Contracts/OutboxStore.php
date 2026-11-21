<?php

declare(strict_types=1);

namespace Outbox\Contracts;

use Outbox\OutboxMessage;

/**
 * Persistence for outbox messages.
 *
 * IMPORTANT: append() must run on the SAME connection/transaction as the business
 * write, otherwise the outbox guarantee is lost.
 */
interface OutboxStore
{
    public function append(OutboxMessage ...$messages): void;

    /**
     * Atomically claim up to $limit due messages for this worker. Claimed messages
     * are leased for $leaseSeconds; if the worker dies the lease expires and another
     * worker can pick them up (at-least-once delivery).
     *
     * @return list<OutboxMessage>
     */
    public function claim(int $limit, string $workerId, int $leaseSeconds): array;

    public function markPublished(string $id): void;

    /**
     * Release the lease and schedule a retry at $retryAt.
     */
    public function markFailed(string $id, string $error, \DateTimeImmutable $retryAt): void;

    /**
     * Give up on a message (dead letter). It stays in the table for inspection/replay.
     */
    public function markDead(string $id, string $error): void;

    /**
     * Move dead messages back to pending (manual replay).
     *
     * @param list<string>|null $ids null = all dead messages
     */
    public function retryDead(?array $ids = null): int;

    /**
     * Delete published messages older than the given date. Returns number deleted.
     */
    public function prune(\DateTimeImmutable $olderThan): int;
}
