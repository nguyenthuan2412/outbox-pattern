<?php

declare(strict_types=1);

namespace Outbox;

/**
 * A single event recorded in the outbox table.
 *
 * Messages are created by the application inside the same DB transaction as the
 * business change, and later read back by the relay which publishes them.
 */
final class OutboxMessage
{
    public const STATUS_PENDING = 'pending';
    public const STATUS_PUBLISHED = 'published';
    public const STATUS_DEAD = 'dead';

    /**
     * @param array<string, mixed>  $payload
     * @param array<string, string> $headers
     */
    public function __construct(
        public readonly string $id,
        public readonly string $eventType,
        public readonly array $payload,
        public readonly ?string $aggregateType = null,
        public readonly ?string $aggregateId = null,
        public readonly array $headers = [],
        public readonly \DateTimeImmutable $createdAt = new \DateTimeImmutable(),
        public readonly \DateTimeImmutable $availableAt = new \DateTimeImmutable(),
        public readonly int $attempts = 0,
        public readonly string $status = self::STATUS_PENDING,
        public readonly ?string $lastError = null,
        public readonly ?string $topic = null,
    ) {
    }

    /**
     * Convenience factory that generates an id (UUIDv7, time-ordered).
     *
     * @param array<string, mixed>  $payload
     * @param array<string, string> $headers
     */
    public static function create(
        string $eventType,
        array $payload,
        ?string $aggregateType = null,
        string|int|null $aggregateId = null,
        array $headers = [],
        ?string $topic = null,
        ?\DateTimeImmutable $availableAt = null,
    ): self {
        $now = new \DateTimeImmutable();

        return new self(
            id: Uuid::v7(),
            eventType: $eventType,
            payload: $payload,
            aggregateType: $aggregateType,
            aggregateId: $aggregateId === null ? null : (string) $aggregateId,
            headers: $headers,
            createdAt: $now,
            availableAt: $availableAt ?? $now,
            topic: $topic,
        );
    }

    /** @param array<string, string> $headers */
    public function withHeaders(array $headers): self
    {
        return $this->with(headers: $headers);
    }

    /**
     * Copy with some fields changed (stores use this when hydrating state transitions).
     *
     * @param array<string, string>|null $headers
     */
    public function with(
        ?string $status = null,
        ?int $attempts = null,
        ?\DateTimeImmutable $availableAt = null,
        ?string $lastError = null,
        ?array $headers = null,
    ): self {
        return new self(
            id: $this->id,
            eventType: $this->eventType,
            payload: $this->payload,
            aggregateType: $this->aggregateType,
            aggregateId: $this->aggregateId,
            headers: $headers ?? $this->headers,
            createdAt: $this->createdAt,
            availableAt: $availableAt ?? $this->availableAt,
            attempts: $attempts ?? $this->attempts,
            status: $status ?? $this->status,
            lastError: $lastError ?? $this->lastError,
            topic: $this->topic,
        );
    }

    /**
     * Key that consumers can use for partitioning / ordering (e.g. Kafka message key).
     */
    public function partitionKey(): ?string
    {
        if ($this->aggregateId === null) {
            return null;
        }

        return ($this->aggregateType ?? '') . ':' . $this->aggregateId;
    }
}
