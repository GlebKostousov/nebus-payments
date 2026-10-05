-- Только чтение: не удаляет сообщения и не меняет лимит попыток.
SELECT count(*) AS ready_unpublished,
       min(available_at) AS oldest_ready_at,
       clock_timestamp() - min(available_at) AS oldest_ready_age
FROM outbox
WHERE published_at IS NULL AND available_at <= clock_timestamp();

SELECT p.id, p.status, p.processing_state, p.attempt_count,
       p.last_error_code, p.last_error_stage, o.available_at,
       o.published_at, o.publish_attempt_count
FROM payments AS p
LEFT JOIN outbox AS o ON o.id = p.expected_message_id
WHERE p.processing_state != 'completed'
ORDER BY p.created_at
LIMIT 100;

SELECT id, attempt_count, lease_until
FROM payments
WHERE processing_state = 'running' AND lease_until < clock_timestamp()
ORDER BY lease_until
LIMIT 100;
