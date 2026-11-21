<?php

declare(strict_types=1);

namespace Outbox\Middleware;

use Outbox\Contracts\PublishMiddleware;
use Outbox\OutboxMessage;

/**
 * Inline middleware: new CallbackMiddleware(fn ($message, $next) => $next($message))
 */
final class CallbackMiddleware implements PublishMiddleware
{
    /** @var \Closure(OutboxMessage, callable): void */
    private \Closure $callback;

    /** @param callable(OutboxMessage, callable(OutboxMessage): void): void $callback */
    public function __construct(callable $callback)
    {
        $this->callback = $callback(...);
    }

    public function process(OutboxMessage $message, callable $next): void
    {
        ($this->callback)($message, $next);
    }
}
