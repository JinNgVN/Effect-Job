import { PgClient } from "@effect/sql-pg";
import { Effect, Layer, Option } from "effect";

import {
    DuplicateJobError,
    JobEngine,
    JobStorageError,
    normalizeJobErrors,
} from "../engine";
import type {
    JobError,
    JobListOptions,
    JobPruneOptions,
    JobRecord,
    JobRescueOptions,
    JobRescueResult,
    JobStatus,
    NewJob,
    WorkerId,
} from "../model";
import {
    postgresTable,
    type PostgresSchemaOptions,
} from "./postgresSchema";

export interface PostgresOptions
    extends PostgresSchemaOptions { }

interface JobRow {
    readonly id: string;
    readonly name: string;
    readonly queue: string;
    readonly payload: unknown;
    readonly meta: unknown;
    readonly tags: ReadonlyArray<string>;
    readonly status: JobStatus;
    readonly priority: number;
    readonly attempt: number;
    readonly executions: number;
    readonly snoozes: number;
    readonly max_attempts: number;
    readonly run_at: Date | string;
    readonly idempotency_key: string | null;
    readonly attempted_at: Date | string | null;
    readonly attempted_by: ReadonlyArray<string>;
    readonly completed_at: Date | string | null;
    readonly cancelled_at: Date | string | null;
    readonly discarded_at: Date | string | null;
    readonly errors: unknown;
    readonly inserted_at: Date | string;
    readonly updated_at: Date | string;
}

export class PostgresNotImplementedError extends Error {
    override readonly name = "PostgresNotImplementedError";

    constructor(operation: string) {
        super(`postgres(...).${operation} is not implemented yet.`);
    }
}

const activeStatuses = [
    "suspended",
    "available",
    "scheduled",
    "executing",
    "retryable",
];

const asDate = (value: Date | string): Date =>
    value instanceof Date ? value : new Date(value);

const asNullableDate = (value: Date | string | null): Date | null =>
    value === null ? null : asDate(value);

const asRecord = (value: unknown): Record<string, unknown> =>
    typeof value === "object" && value !== null && !Array.isArray(value)
        ? (value as Record<string, unknown>)
        : {};

const asErrors = (value: unknown): ReadonlyArray<JobError> =>
    Array.isArray(value) ? (value as ReadonlyArray<JobError>) : [];

export const postgresRowToJobRecord = (row: JobRow): JobRecord => ({
    id: row.id,
    name: row.name,
    queue: row.queue,
    payload: row.payload,
    meta: asRecord(row.meta),
    tags: row.tags,
    status: row.status,
    priority: row.priority,
    attempt: row.attempt,
    executions: row.executions,
    snoozes: row.snoozes,
    maxAttempts: row.max_attempts,
    runAt: asDate(row.run_at),
    idempotencyKey: row.idempotency_key,
    attemptedAt: asNullableDate(row.attempted_at),
    attemptedBy: row.attempted_by,
    completedAt: asNullableDate(row.completed_at),
    cancelledAt: asNullableDate(row.cancelled_at),
    discardedAt: asNullableDate(row.discarded_at),
    errors: asErrors(row.errors),
    insertedAt: asDate(row.inserted_at),
    updatedAt: asDate(row.updated_at),
});

const storageError = (operation: string) => (cause: unknown) =>
    new JobStorageError({ operation, cause });

const unsupported = (operation: string) =>
    Effect.fail(
        storageError(operation)(new PostgresNotImplementedError(operation)),
    );

const statusFor = (job: NewJob): JobStatus =>
    job.runAt.getTime() > Date.now() ? "scheduled" : "available";

const activeDuplicateSql = (table: string) => `
SELECT *
FROM ${table}
WHERE name = $1
  AND queue = $2
  AND idempotency_key = $3
  AND status = ANY($4::text[])
LIMIT 1
`;

const findActiveDuplicate = (
    sql: PgClient.PgClient,
    table: string,
    job: NewJob,
) =>
    job.idempotencyKey === undefined
        ? Effect.succeed(Option.none<JobRecord>())
        : sql
            .unsafe<JobRow>(activeDuplicateSql(table), [
                job.name,
                job.queue,
                job.idempotencyKey,
                activeStatuses,
            ])
            .pipe(
                Effect.map((rows) =>
                    Option.fromNullishOr(rows[0]).pipe(
                        Option.map(postgresRowToJobRecord),
                    ),
                ),
            );

const insertSql = (table: string) => `
INSERT INTO ${table} (
  id,
  name,
  queue,
  payload,
  meta,
  tags,
  status,
  priority,
  attempt,
  executions,
  snoozes,
  max_attempts,
  run_at,
  idempotency_key,
  errors
)
VALUES (
  $1,
  $2,
  $3,
  $4::jsonb,
  $5::jsonb,
  $6::text[],
  $7,
  $8,
  0,
  0,
  0,
  $9,
  $10,
  $11,
  '[]'::jsonb
)
ON CONFLICT (name, queue, idempotency_key)
WHERE idempotency_key IS NOT NULL
  AND status IN ('suspended', 'available', 'scheduled', 'executing', 'retryable')
DO NOTHING
RETURNING *
`;

const listSql = (table: string, options?: JobListOptions) => {
    const clauses: Array<string> = [];
    const params: Array<unknown> = [];

    if (options?.queue !== undefined) {
        params.push(
            Array.isArray(options.queue) ? options.queue : [options.queue],
        );
        clauses.push(`queue = ANY($${params.length}::text[])`);
    }

    if (options?.status !== undefined) {
        params.push(
            Array.isArray(options.status) ? options.status : [options.status],
        );
        clauses.push(`status = ANY($${params.length}::text[])`);
    }

    const where = clauses.length === 0 ? "" : `WHERE ${clauses.join(" AND ")}`;

    if (options?.limit !== undefined) {
        params.push(options.limit);
    }

    return {
        sql: `
SELECT *
FROM ${table}
${where}
ORDER BY inserted_at ASC
${options?.limit === undefined ? "" : `LIMIT $${params.length}`}
`,
        params,
    };
};

const claimNextSql = (table: string) => `
WITH selected AS (
  SELECT id
  FROM ${table}
  WHERE ($1::text IS NULL OR queue = $1)
    AND status IN ('available', 'scheduled', 'retryable')
    AND run_at <= now()
  ORDER BY priority ASC, run_at ASC, inserted_at ASC
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
UPDATE ${table}
SET status = 'executing',
    executions = executions + 1,
    attempted_at = now(),
    attempted_by = CASE
      WHEN $2::text IS NULL THEN attempted_by
      ELSE array_append(attempted_by, $2::text)
    END,
    updated_at = now()
WHERE id IN (SELECT id FROM selected)
RETURNING *
`;

const completeSql = (table: string) => `
UPDATE ${table}
SET status = 'completed',
    completed_at = now(),
    updated_at = now()
WHERE id = $1
`;

const failSql = (table: string) => `
UPDATE ${table}
SET status = CASE
      WHEN $3::boolean OR attempt + 1 >= max_attempts THEN 'discarded'
      ELSE 'retryable'
    END,
    attempt = attempt + 1,
    run_at = COALESCE($4::timestamptz, run_at),
    discarded_at = CASE
      WHEN $3::boolean OR attempt + 1 >= max_attempts THEN now()
      ELSE NULL
    END,
    errors = errors || $2::jsonb,
    updated_at = now()
WHERE id = $1
`;

const cancelSql = (table: string) => `
UPDATE ${table}
SET status = 'cancelled',
    cancelled_at = now(),
    errors = errors || $2::jsonb,
    updated_at = now()
WHERE id = $1
`;

const snoozeSql = (table: string) => `
UPDATE ${table}
SET status = 'scheduled',
    run_at = $2,
    snoozes = snoozes + 1,
    updated_at = now()
WHERE id = $1
`;

const runNowSql = (table: string) => `
UPDATE ${table}
SET status = 'available',
    run_at = now(),
    updated_at = now()
WHERE id = $1
  AND status IN ('scheduled', 'retryable')
`;

const pruneSql = (table: string) => `
DELETE FROM ${table}
WHERE status = ANY($1::text[])
  AND updated_at < $2
RETURNING id
`;

const rescueSql = (table: string) => `
WITH selected AS (
  SELECT *
  FROM ${table}
  WHERE status = 'executing'
    AND attempted_at < $1
  FOR UPDATE SKIP LOCKED
),
updated AS (
  UPDATE ${table}
  SET status = CASE
        WHEN selected.attempt >= selected.max_attempts THEN 'discarded'
        ELSE 'available'
      END,
      discarded_at = CASE
        WHEN selected.attempt >= selected.max_attempts THEN now()
        ELSE ${table}.discarded_at
      END,
      run_at = CASE
        WHEN selected.attempt >= selected.max_attempts THEN ${table}.run_at
        ELSE now()
      END,
      updated_at = now()
  FROM selected
  WHERE ${table}.id = selected.id
  RETURNING ${table}.*
)
SELECT * FROM updated
`;

const makeEngine = (schemaOptions: PostgresSchemaOptions) =>
    Effect.gen(function* () {
        const sql = yield* PgClient.PgClient;
        const table = postgresTable(schemaOptions);

        return {
            enqueue: (job: NewJob) =>
                Effect.gen(function* () {
                    const existing = yield* findActiveDuplicate(
                        sql,
                        table,
                        job,
                    ).pipe(Effect.mapError(storageError("enqueue")));

                    if (Option.isSome(existing)) {
                        if (job.duplicatePolicy === "fail") {
                            return yield* new DuplicateJobError({
                                existing: existing.value,
                            });
                        }

                        return existing.value;
                    }

                    const rows = yield* sql
                        .unsafe<JobRow>(insertSql(table), [
                            job.id,
                            job.name,
                            job.queue,
                            JSON.stringify(job.payload),
                            JSON.stringify(job.meta),
                            job.tags,
                            statusFor(job),
                            job.priority,
                            job.maxAttempts,
                            job.runAt,
                            job.idempotencyKey ?? null,
                        ])
                        .pipe(Effect.mapError(storageError("enqueue")));

                    if (rows[0] !== undefined) {
                        return postgresRowToJobRecord(rows[0]);
                    }

                    const raced = yield* findActiveDuplicate(
                        sql,
                        table,
                        job,
                    ).pipe(Effect.mapError(storageError("enqueue")));

                    if (Option.isSome(raced)) {
                        if (job.duplicatePolicy === "fail") {
                            return yield* new DuplicateJobError({
                                existing: raced.value,
                            });
                        }

                        return raced.value;
                    }

                    return yield* storageError("enqueue")(
                        "insert returned no row",
                    );
                }),
            find: (id: string) =>
                sql
                    .unsafe<JobRow>(`SELECT * FROM ${table} WHERE id = $1 LIMIT 1`, [
                        id,
                    ])
                    .pipe(
                        Effect.map((rows) =>
                            Option.fromNullishOr(rows[0]).pipe(
                                Option.map(postgresRowToJobRecord),
                            ),
                        ),
                        Effect.catch((cause: unknown) =>
                            Effect.fail(storageError("find")(cause)),
                        ),
                    ),
            list: (options?: JobListOptions) => {
                const query = listSql(table, options);

                return sql.unsafe<JobRow>(query.sql, query.params).pipe(
                    Effect.map((rows) => rows.map(postgresRowToJobRecord)),
                    Effect.catch((cause: unknown) =>
                        Effect.fail(storageError("list")(cause)),
                    ),
                );
            },
            claimNext: (options?: {
                readonly queue?: string;
                readonly workerId?: WorkerId;
            }) =>
                sql
                    .unsafe<JobRow>(claimNextSql(table), [
                        options?.queue ?? null,
                        options?.workerId ?? null,
                    ])
                    .pipe(
                        Effect.map((rows) =>
                            Option.fromNullishOr(rows[0]).pipe(
                                Option.map(postgresRowToJobRecord),
                            ),
                        ),
                        Effect.catch((cause: unknown) =>
                            Effect.fail(storageError("claimNext")(cause)),
                        ),
                    ),
            complete: (id: string) =>
                sql.unsafe(completeSql(table), [id]).pipe(
                    Effect.asVoid,
                    Effect.catch((cause: unknown) =>
                        Effect.fail(storageError("complete")(cause)),
                    ),
                ),
            fail: (
                id: string,
                error: unknown,
                options?: { readonly runAt?: Date; readonly discard?: boolean },
            ) =>
                Effect.gen(function* () {
                    const current = yield* sql
                        .unsafe<JobRow>(`SELECT * FROM ${table} WHERE id = $1 LIMIT 1`, [
                            id,
                        ])
                        .pipe(Effect.mapError(storageError("fail")));
                    const record = current[0];

                    if (record === undefined) {
                        return;
                    }

                    const nextAttempt = record.attempt + 1;
                    const errors = normalizeJobErrors(
                        error,
                        nextAttempt,
                        new Date(),
                    );

                    yield* sql
                        .unsafe(failSql(table), [
                            id,
                            JSON.stringify(errors),
                            options?.discard === true,
                            options?.runAt ?? null,
                        ])
                        .pipe(
                            Effect.asVoid,
                            Effect.mapError(storageError("fail")),
                        );
                }),
            cancel: (id: string, reason: unknown) =>
                Effect.gen(function* () {
                    const current = yield* sql
                        .unsafe<JobRow>(`SELECT * FROM ${table} WHERE id = $1 LIMIT 1`, [
                            id,
                        ])
                        .pipe(Effect.mapError(storageError("cancel")));
                    const record = current[0];

                    if (record === undefined) {
                        return;
                    }

                    const errors = normalizeJobErrors(
                        reason,
                        record.attempt,
                        new Date(),
                    );

                    yield* sql
                        .unsafe(cancelSql(table), [
                            id,
                            JSON.stringify(errors),
                        ])
                        .pipe(
                            Effect.asVoid,
                            Effect.mapError(storageError("cancel")),
                        );
                }),
            snooze: (id: string, runAt: Date) =>
                sql.unsafe(snoozeSql(table), [id, runAt]).pipe(
                    Effect.asVoid,
                    Effect.catch((cause: unknown) =>
                        Effect.fail(storageError("snooze")(cause)),
                    ),
                ),
            runNow: (id: string) =>
                sql.unsafe(runNowSql(table), [id]).pipe(
                    Effect.asVoid,
                    Effect.catch((cause: unknown) =>
                        Effect.fail(storageError("runNow")(cause)),
                    ),
                ),
            prune: (options: JobPruneOptions) => {
                const statuses = options.statuses ?? [
                    "completed",
                    "cancelled",
                    "discarded",
                ];

                return sql
                    .unsafe<{ readonly id: string }>(pruneSql(table), [
                        statuses,
                        options.before,
                    ])
                    .pipe(
                        Effect.map((rows) => rows.length),
                        Effect.catch((cause: unknown) =>
                            Effect.fail(storageError("prune")(cause)),
                        ),
                    );
            },
            rescueExecuting: (
                options: JobRescueOptions,
            ): Effect.Effect<JobRescueResult, JobStorageError> =>
                sql.unsafe<JobRow>(rescueSql(table), [options.before]).pipe(
                    Effect.map((rows) => {
                        const records = rows.map(postgresRowToJobRecord);

                        return {
                            rescued: records.filter(
                                (record) => record.status === "available",
                            ),
                            discarded: records.filter(
                                (record) => record.status === "discarded",
                            ),
                        };
                    }),
                    Effect.catch((cause: unknown) =>
                        Effect.fail(storageError("rescueExecuting")(cause)),
                    ),
                ),
        };
    });

export const postgres = (
    options: PostgresOptions = {},
): Layer.Layer<JobEngine, never, PgClient.PgClient> =>
    Layer.effect(JobEngine)(makeEngine(options));

export const JobEnginePostgres = {
    layer: postgres,
};
