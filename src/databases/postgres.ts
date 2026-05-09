import { PgClient } from "@effect/sql-pg";
import { Effect, Layer, Option } from "effect";

import {
    DuplicateJobError,
    JobEngine,
    JobStorageError,
} from "../engine";
import type {
    JobError,
    JobListOptions,
    JobRecord,
    JobStatus,
    NewJob,
} from "../model";
import {
    postgresTable,
    type PostgresSchemaOptions,
} from "./postgresSchema";

export interface PostgresOptions
    extends PostgresSchemaOptions,
    PgClient.PgPoolConfig { }

export type PostgresDatabaseInput =
    | PostgresOptions
    | Layer.Layer<PgClient.PgClient, any, any>;

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

const isPgLayer = (
    input: PostgresDatabaseInput,
): input is Layer.Layer<PgClient.PgClient, any, any> =>
    typeof input === "object" &&
    input !== null &&
    "pipe" in input &&
    !("url" in input) &&
    !("host" in input) &&
    !("database" in input);

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
            claimNext: () => unsupported("claimNext"),
            complete: () => unsupported("complete"),
            fail: () => unsupported("fail"),
            cancel: () => unsupported("cancel"),
            snooze: () => unsupported("snooze"),
            runNow: () => unsupported("runNow"),
            prune: () => unsupported("prune"),
            rescueExecuting: () => unsupported("rescueExecuting"),
        };
    });

export const postgres = (input: PostgresDatabaseInput = {}) => {
    const schemaOptions = isPgLayer(input) ? {} : input;
    const engine = Layer.effect(JobEngine)(makeEngine(schemaOptions));

    if (isPgLayer(input)) {
        return engine.pipe(Layer.provide(input));
    }

    const { schema: _schema, table: _table, ...pgOptions } = input;

    return engine.pipe(Layer.provide(PgClient.layer(pgOptions)));
};
