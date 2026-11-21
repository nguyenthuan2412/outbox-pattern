<?php

declare(strict_types=1);

namespace Outbox\Exception;

/**
 * Throw (or implement) this from a Publisher to send the message straight to dead letter,
 * e.g. on schema/validation errors where retrying can never succeed.
 */
class NonRetryableException extends \RuntimeException implements OutboxException
{
}
