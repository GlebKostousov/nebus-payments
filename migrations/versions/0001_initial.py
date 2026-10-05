"""Первоначальная схема платежей и outbox. Ревизия не импортирует текущие модели."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Создать две таблицы и ограничения согласованности сохранённых состояний."""
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("metadata", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("request_fingerprint", sa.Text(), nullable=False),
        sa.Column("webhook_url", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provider_reference", sa.Text(), nullable=True),
        sa.Column("webhook_event_id", sa.Uuid(), nullable=True),
        sa.Column("webhook_payload", sa.Text(), nullable=True),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "processing_state", sa.Text(), nullable=False, server_default="ready"
        ),
        sa.Column(
            "attempt_count", sa.SmallInteger(), nullable=False, server_default="0"
        ),
        sa.Column("expected_message_id", sa.Uuid(), nullable=False),
        sa.Column("lease_token", sa.Uuid(), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_code", sa.Text(), nullable=True),
        sa.Column("last_error_stage", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "attempt_count BETWEEN 0 AND 3", name=op.f("ck_payments_attempt_budget")
        ),
        sa.CheckConstraint(
            "currency IN ('RUB', 'USD', 'EUR')",
            name=op.f("ck_payments_currency_values"),
        ),
        sa.CheckConstraint(
            "(processing_state = 'completed' AND webhook_delivered_at IS NOT NULL AND status != 'pending') OR (processing_state != 'completed' AND webhook_delivered_at IS NULL)",
            name=op.f("ck_payments_delivery_complete"),
        ),
        sa.CheckConstraint(
            "(processing_state = 'running' AND lease_token IS NOT NULL AND lease_until IS NOT NULL) OR (processing_state != 'running' AND lease_token IS NULL AND lease_until IS NULL)",
            name=op.f("ck_payments_lease_pair"),
        ),
        sa.CheckConstraint("amount > 0", name=op.f("ck_payments_positive_amount")),
        sa.CheckConstraint(
            "processing_state IN ('ready', 'running', 'completed', 'dead_lettered')",
            name=op.f("ck_payments_processing_state_values"),
        ),
        sa.CheckConstraint(
            "processing_state != 'ready' OR attempt_count < 3",
            name=op.f("ck_payments_ready_budget"),
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL AND webhook_payload IS NULL AND webhook_event_id IS NULL AND provider_reference IS NULL) OR (status != 'pending' AND processed_at IS NOT NULL AND webhook_payload IS NOT NULL AND webhook_event_id IS NOT NULL AND provider_reference IS NOT NULL)",
            name=op.f("ck_payments_result_complete"),
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')",
            name=op.f("ck_payments_status_values"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_payments")),
        sa.UniqueConstraint(
            "idempotency_key", name=op.f("uq_payments_idempotency_key")
        ),
        sa.UniqueConstraint(
            "webhook_event_id", name=op.f("uq_payments_webhook_event_id")
        ),
    )
    op.create_index(
        "ix_payments_expired_work",
        "payments",
        ["lease_until", "id"],
        postgresql_where=sa.text("processing_state = 'running'"),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("payment_id", sa.Uuid(), nullable=False),
        sa.Column("destination", sa.Text(), nullable=False),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.Column("deduplication_key", sa.Text(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "publish_attempt_count", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column("lease_token", sa.Uuid(), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_code", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "publish_attempt_count >= 0", name=op.f("ck_outbox_attempts_nonnegative")
        ),
        sa.CheckConstraint(
            "destination IN ('processing', 'dead_letter')",
            name=op.f("ck_outbox_destination_values"),
        ),
        sa.CheckConstraint(
            "(lease_token IS NULL AND lease_until IS NULL) OR (lease_token IS NOT NULL AND lease_until IS NOT NULL)",
            name=op.f("ck_outbox_lease_pair"),
        ),
        sa.CheckConstraint(
            "published_at IS NULL OR lease_token IS NULL",
            name=op.f("ck_outbox_published_unlocked"),
        ),
        sa.ForeignKeyConstraint(
            ["payment_id"], ["payments.id"], name=op.f("fk_outbox_payment_id_payments")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox")),
        sa.UniqueConstraint(
            "deduplication_key", name=op.f("uq_outbox_deduplication_key")
        ),
    )
    op.create_index(
        "ix_outbox_available",
        "outbox",
        ["available_at", "id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_index("ix_outbox_payment_id", "outbox", ["payment_id"])


def downgrade() -> None:
    """Удалить таблицы платежей и outbox."""
    op.drop_table("outbox")
    op.drop_table("payments")
