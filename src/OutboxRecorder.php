<?php

declare(strict_types=1);

namespace Outbox;

use Outbox\Contracts\OutboxStore;
use Outbox\Contracts\TransactionRunner;

/**
 * Write side: what application code uses to record events.
 *
 *   $outbox->transactional(function (OutboxRecorder $outbox) use ($order) {
 *       $orders->save($order);
 *       $outbox->record(OutboxMessage::create('order.created', [...], 'order', $order->id));
 *   });
 *
 * If you already manage the transaction yourself, just call record() inside it.
 */
final class OutboxRecorder
{
    public function __construct(
        private readonly OutboxStore $store,
        private readonly ?TransactionRunner $transactions = null,
    ) {
    }

    public function record(OutboxMessage ...$messages): void
    {
        $this->store->append(...$messages);
    }

    /**
     * Run $callback in a transaction; messages recorded inside it commit (or roll back) with it.
     *
     * @template T
     * @param callable(self): T $callback
     * @return T
     */
    public function transactional(callable $callback): mixed
    {
        if ($this->transactions === null) {
            throw new \LogicException('No TransactionRunner configured; pass one to OutboxRecorder or manage the transaction yourself.');
        }

        return $this->transactions->transactional(fn () => $callback($this));
    }
}
