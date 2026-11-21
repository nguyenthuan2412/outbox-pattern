<?php

declare(strict_types=1);

namespace Outbox\Middleware;

use Outbox\Contracts\PublishMiddleware;
use Outbox\Contracts\Publisher;
use Outbox\OutboxMessage;

/**
 * Composes middlewares around a Publisher. Itself a Publisher, so it can be nested.
 * Middlewares run in the order given (first = outermost).
 */
final class Pipeline implements Publisher
{
    /** @var list<PublishMiddleware> */
    private array $middlewares;

    public function __construct(
        private readonly Publisher $publisher,
        PublishMiddleware ...$middlewares,
    ) {
        $this->middlewares = array_values($middlewares);
    }

    public function publish(OutboxMessage $message): void
    {
        $this->dispatch(0, $message);
    }

    private function dispatch(int $index, OutboxMessage $message): void
    {
        if (!isset($this->middlewares[$index])) {
            $this->publisher->publish($message);

            return;
        }

        $this->middlewares[$index]->process($message, fn (OutboxMessage $m) => $this->dispatch($index + 1, $m));
    }
}
