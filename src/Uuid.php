<?php

declare(strict_types=1);

namespace Outbox;

/**
 * Minimal dependency-free UUIDv7 generator (RFC 9562).
 * Time-ordered ids keep the outbox index append-friendly.
 */
final class Uuid
{
    public static function v7(): string
    {
        $ms = (int) floor(microtime(true) * 1000);
        $bytes = pack('J', $ms);            // 8 bytes big endian
        $bytes = substr($bytes, 2, 6);      // 48-bit timestamp
        $bytes .= random_bytes(10);

        $bytes[6] = chr((ord($bytes[6]) & 0x0F) | 0x70); // version 7
        $bytes[8] = chr((ord($bytes[8]) & 0x3F) | 0x80); // variant RFC

        $hex = bin2hex($bytes);

        return sprintf(
            '%s-%s-%s-%s-%s',
            substr($hex, 0, 8),
            substr($hex, 8, 4),
            substr($hex, 12, 4),
            substr($hex, 16, 4),
            substr($hex, 20, 12),
        );
    }
}
