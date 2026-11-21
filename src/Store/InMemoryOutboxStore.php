<?php

declare(strict_types=1);

namespace Outbox\Store;

use Outbox\Contracts\OutboxStore;
use Outbox\OutboxMessage;

/**
 * Non-persistent reference implementation of OutboxStore.
 *
 * Use it in unit tests, and read it as the spec for your own store
 * (SQL, Mongo, DynamoDB, ...). Not safe across processes.
 */
class InMemoryOutboxStore implements OutboxStore
{
    /** @var array<string, array{message: OutboxMessage, lockedUntil: ?float, publishedAt: ?\DateTimeImmutable}> */
    private array $rows = [];

    public function append(OutboxMessage ...$messages): void
    {
        foreach ($messages as $m) {
            $this->rows[$m->id] = ['message' => $m, 'lockedUntil' => null, 'publishedAt' => null];
        }
    }

    public function claim(int $limit, string $workerId, int $leaseSeconds): array
    {
        $now = microtime(true);
        $nowDt = new \DateTimeImmutable();
        $out = [];
        foreach ($this->rows as $id => $row) {
            if (count($out) >= $limit) {
                break;
            }
            $m = $row['message'];
            if ($m->status !== OutboxMessage::STATUS_PENDING || $m->availableAt > $nowDt) {
                continue;
            }
            if ($row['lockedUntil'] !== null && $row['lockedUntil'] > $now) {
                continue;
            }
            $m = $m->with(attempts: $m->attempts + 1);
            $this->rows[$id] = ['message' => $m, 'lockedUntil' => $now + $leaseSeconds, 'publishedAt' => null];
            $out[] = $m;
        }

        return $out;
    }

    public function markPublished(string $id): void
    {
        if (isset($this->rows[$id])) {
            $this->rows[$id]['message'] = $this->rows[$id]['message']->with(status: OutboxMessage::STATUS_PUBLISHED);
            $this->rows[$id]['lockedUntil'] = null;
            $this->rows[$id]['publishedAt'] = new \DateTimeImmutable();
        }
    }

    public function markFailed(string $id, string $error, \DateTimeImmutable $retryAt): void
    {
        if (isset($this->rows[$id])) {
            $this->rows[$id]['message'] = $this->rows[$id]['message']->with(availableAt: $retryAt, lastError: $error);
            $this->rows[$id]['lockedUntil'] = null;
        }
    }

    public function markDead(string $id, string $error): void
    {
        if (isset($this->rows[$id])) {
            $this->rows[$id]['message'] = $this->rows[$id]['message']->with(status: OutboxMessage::STATUS_DEAD, lastError: $error);
            $this->rows[$id]['lockedUntil'] = null;
        }
    }

    public function retryDead(?array $ids = null): int
    {
        $n = 0;
        foreach ($this->rows as $id => $row) {
            if ($row['message']->status === OutboxMessage::STATUS_DEAD && ($ids === null || in_array($id, $ids, true))) {
                $this->rows[$id]['message'] = $row['message']->with(status: OutboxMessage::STATUS_PENDING, attempts: 0, availableAt: new \DateTimeImmutable());
                ++$n;
            }
        }

        return $n;
    }

    public function prune(\DateTimeImmutable $olderThan): int
    {
        $n = 0;
        foreach ($this->rows as $id => $row) {
            if ($row['publishedAt'] !== null && $row['publishedAt'] < $olderThan) {
                unset($this->rows[$id]);
                ++$n;
            }
        }

        return $n;
    }

    /** @return list<OutboxMessage> */
    public function all(): array
    {
        return array_values(array_map(static fn (array $r) => $r['message'], $this->rows));
    }
}
