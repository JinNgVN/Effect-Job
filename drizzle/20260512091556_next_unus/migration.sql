CREATE TABLE "effect_jobs" (
	"id" text PRIMARY KEY,
	"name" text NOT NULL,
	"queue" text DEFAULT 'default' NOT NULL,
	"payload" jsonb DEFAULT '{}' NOT NULL,
	"meta" jsonb DEFAULT '{}' NOT NULL,
	"tags" text[] DEFAULT ARRAY[]::text[] NOT NULL,
	"status" text NOT NULL,
	"priority" integer DEFAULT 0 NOT NULL,
	"attempt" integer DEFAULT 0 NOT NULL,
	"executions" integer DEFAULT 0 NOT NULL,
	"snoozes" integer DEFAULT 0 NOT NULL,
	"max_attempts" integer DEFAULT 20 NOT NULL,
	"run_at" timestamp with time zone DEFAULT now() NOT NULL,
	"idempotency_key" text,
	"attempted_at" timestamp with time zone,
	"attempted_by" text[] DEFAULT ARRAY[]::text[] NOT NULL,
	"completed_at" timestamp with time zone,
	"cancelled_at" timestamp with time zone,
	"discarded_at" timestamp with time zone,
	"errors" jsonb DEFAULT '[]' NOT NULL,
	"inserted_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "effect_jobs_status_check" CHECK ("status" IN ('suspended', 'available', 'scheduled', 'executing', 'retryable', 'completed', 'discarded', 'cancelled')),
	CONSTRAINT "effect_jobs_attempt_check" CHECK ("attempt" >= 0 AND "executions" >= 0 AND "snoozes" >= 0 AND "max_attempts" >= 1)
);
--> statement-breakpoint
CREATE INDEX "effect_jobs_ready_idx" ON "effect_jobs" ("queue","priority","run_at","inserted_at") WHERE "status" IN ('available', 'scheduled', 'retryable');--> statement-breakpoint
CREATE INDEX "effect_jobs_status_queue_idx" ON "effect_jobs" ("status","queue");--> statement-breakpoint
CREATE INDEX "effect_jobs_executing_idx" ON "effect_jobs" ("attempted_at") WHERE "status" = 'executing';--> statement-breakpoint
CREATE UNIQUE INDEX "effect_jobs_idempotency_idx" ON "effect_jobs" ("name","queue","idempotency_key") WHERE "idempotency_key" IS NOT NULL AND "status" IN ('suspended', 'available', 'scheduled', 'executing', 'retryable');