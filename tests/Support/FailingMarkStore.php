<?php

declare(strict_types=1);

namespace Outbox\Tests\Support;

use Outbox\Contracts\OutboxStore;
use Outbox\OutboxMessage;
use Outbox\Store\InMemoryOutboxStore;

class FailingMarkStore implements OutboxStore
{
    private InMemoryOutboxStore $inner;

    public function __construct()
    {
        $this->inner = new InMemoryOutboxStore();
    }

    public function append(OutboxMessage ...$messages): void { $this->inner->append(...$messages); }
    public function claim(int $limit, string $workerId, int $leaseSeconds): array { return $this->inner->claim($limit, $workerId, $leaseSeconds); }
    public function markPublished(string $id): void { throw new \RuntimeException('db gone'); }
    public function markFailed(string $id, string $error, \DateTimeImmutable $retryAt): void { throw new \RuntimeException('db gone'); }
    public function markDead(string $id, string $error): void { throw new \RuntimeException('db gone'); }
    public function retryDead(?array $ids = null): int { return 0; }
    public function prune(\DateTimeImmutable $olderThan): int { return 0; }
}
