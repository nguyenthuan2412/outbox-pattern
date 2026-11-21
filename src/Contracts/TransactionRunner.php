<?php

declare(strict_types=1);

namespace Outbox\Contracts;

/**
 * Runs a callback inside a DB transaction. Frameworks already have one
 * (DB::transaction, EntityManager::wrapInTransaction, Db::transaction) — the
 * bridges adapt them; PdoTransactionRunner is the plain-PDO fallback.
 */
interface TransactionRunner
{
    /**
     * @template T
     * @param callable(): T $callback
     * @return T
     */
    public function transactional(callable $callback): mixed;
}
