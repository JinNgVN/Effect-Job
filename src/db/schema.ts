import { sql } from "drizzle-orm";
import {
    check,
    index,
    integer,
    jsonb,
    pgTable,
    text,
    timestamp,
    uniqueIndex,
} from "drizzle-orm/pg-core";

export const effectJobStatuses = [
    "suspended",
    "available",
    "scheduled",
    "executing",
    "retryable",
    "completed",
    "discarded",
    "cancelled",
] as const;

export const effectJobActiveStatuses = [
    "suspended",
    "available",
    "scheduled",
    "executing",
    "retryable",
] as const;

const statusSql = effectJobStatuses.map((status) => sql.raw(`'${status}'`));
const activeStatusSql = effectJobActiveStatuses.map((status) =>
    sql.raw(`'${status}'`),
);

export const effectJobs = pgTable(
    "effect_jobs",
    {
        id: text("id").primaryKey(),
        name: text("name").notNull(),
        queue: text("queue").notNull().default("default"),
        payload: jsonb("payload").notNull().default(sql`'{}'::jsonb`),
        meta: jsonb("meta").notNull().default(sql`'{}'::jsonb`),
        tags: text("tags").array().notNull().default(sql`ARRAY[]::text[]`),
        status: text("status", { enum: effectJobStatuses }).notNull(),
        priority: integer("priority").notNull().default(0),
        attempt: integer("attempt").notNull().default(0),
        executions: integer("executions").notNull().default(0),
        snoozes: integer("snoozes").notNull().default(0),
        maxAttempts: integer("max_attempts").notNull().default(20),
        runAt: timestamp("run_at", { withTimezone: true })
            .notNull()
            .defaultNow(),
        idempotencyKey: text("idempotency_key"),
        attemptedAt: timestamp("attempted_at", { withTimezone: true }),
        attemptedBy: text("attempted_by")
            .array()
            .notNull()
            .default(sql`ARRAY[]::text[]`),
        completedAt: timestamp("completed_at", { withTimezone: true }),
        cancelledAt: timestamp("cancelled_at", { withTimezone: true }),
        discardedAt: timestamp("discarded_at", { withTimezone: true }),
        errors: jsonb("errors").notNull().default(sql`'[]'::jsonb`),
        insertedAt: timestamp("inserted_at", { withTimezone: true })
            .notNull()
            .defaultNow(),
        updatedAt: timestamp("updated_at", { withTimezone: true })
            .notNull()
            .defaultNow(),
    },
    (table) => [
        check("effect_jobs_status_check", sql`${table.status} IN (${sql.join(statusSql, sql`, `)})`),
        check(
            "effect_jobs_attempt_check",
            sql`${table.attempt} >= 0 AND ${table.executions} >= 0 AND ${table.snoozes} >= 0 AND ${table.maxAttempts} >= 1`,
        ),
        index("effect_jobs_ready_idx")
            .on(table.queue, table.priority, table.runAt, table.insertedAt)
            .where(sql`${table.status} IN ('available', 'scheduled', 'retryable')`),
        index("effect_jobs_status_queue_idx").on(table.status, table.queue),
        index("effect_jobs_executing_idx")
            .on(table.attemptedAt)
            .where(sql`${table.status} = 'executing'`),
        uniqueIndex("effect_jobs_idempotency_idx")
            .on(table.name, table.queue, table.idempotencyKey)
            .where(
                sql`${table.idempotencyKey} IS NOT NULL AND ${table.status} IN (${sql.join(activeStatusSql, sql`, `)})`,
            ),
    ],
);

export type EffectJobRow = typeof effectJobs.$inferSelect;
export type NewEffectJobRow = typeof effectJobs.$inferInsert;
