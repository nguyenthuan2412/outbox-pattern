<?php

declare(strict_types=1);

namespace Outbox\Tests\Support;

use Outbox\Contracts\Publisher;
use Outbox\OutboxMessage;

final class RecordingPublisher implements Publisher
{
    /** @var list<OutboxMessage> */
    public array $published = [];

    /** @param (\Closure(OutboxMessage): void)|null $behaviour */
    public function __construct(private ?\Closure $behaviour = null)
    {
    }

    public function publish(OutboxMessage $message): void
    {
        if ($this->behaviour !== null) {
            ($this->behaviour)($message);
        }
        $this->published[] = $message;
    }
}
