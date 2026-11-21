<?php

declare(strict_types=1);

namespace Outbox\Relay;

final class BatchResult
{
    public function __construct(
        public int $claimed = 0,
        public int $published = 0,
        public int $retried = 0,
        public int $dead = 0,
    ) {
    }

    public function add(Outcome $outcome): void
    {
        match ($outcome) {
            Outcome::Published => ++$this->published,
            Outcome::Retry => ++$this->retried,
            Outcome::Dead => ++$this->dead,
        };
    }

    public function isEmpty(): bool
    {
        return $this->claimed === 0;
    }
}
