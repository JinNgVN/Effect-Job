export interface PostgresSchemaOptions {
    readonly schema?: string;
    readonly table?: string;
}

export interface PostgresMigration {
    readonly up: string;
    readonly down: string;
}

const defaultSchema = "public";
const defaultTable = "effect_jobs";
const activeStatuses = ["suspended", "available", "scheduled", "executing", "retryable"];
const statuses = [
    ...activeStatuses,
    "completed",
    "discarded",
    "cancelled",
];

export const quotePostgresIdentifier = (identifier: string): string =>
    `"${identifier.replaceAll("\"", "\"\"")}"`;

export const postgresTable = (options: PostgresSchemaOptions = {}): string =>
    `${quotePostgresIdentifier(
        options.schema ?? defaultSchema,
    )}.${quotePostgresIdentifier(options.table ?? defaultTable)}`;

const indexName = (
    options: PostgresSchemaOptions,
    suffix: string,
): string =>
    quotePostgresIdentifier(`${options.table ?? defaultTable}_${suffix}`);

export const postgresMigration = (
    options: PostgresSchemaOptions = {},
): PostgresMigration => {
    const table = postgresTable(options);
    const schema = quotePostgresIdentifier(options.schema ?? defaultSchema);
    const readyIndex = indexName(options, "ready_idx");
    const statusQueueIndex = indexName(options, "status_queue_idx");
    const idempotencyIndex = indexName(options, "idempotency_idx");
    const executingIndex = indexName(options, "executing_idx");
    const statusList = statuses.map((status) => `'${status}'`).join(", ");
    const activeStatusList = activeStatuses
        .map((status) => `'${status}'`)
        .join(", ");

    return {
        up: [
            `CREATE SCHEMA IF NOT EXISTS ${schema};`,
            `CREATE TABLE IF NOT EXISTS ${table} (`,
            `  id text PRIMARY KEY,`,
            `  name text NOT NULL,`,
            `  queue text NOT NULL DEFAULT 'default',`,
            `  payload jsonb NOT NULL DEFAULT '{}'::jsonb,`,
            `  meta jsonb NOT NULL DEFAULT '{}'::jsonb,`,
            `  tags text[] NOT NULL DEFAULT ARRAY[]::text[],`,
            `  status text NOT NULL,`,
            `  priority integer NOT NULL DEFAULT 0,`,
            `  attempt integer NOT NULL DEFAULT 0,`,
            `  executions integer NOT NULL DEFAULT 0,`,
            `  snoozes integer NOT NULL DEFAULT 0,`,
            `  max_attempts integer NOT NULL DEFAULT 20,`,
            `  run_at timestamptz NOT NULL DEFAULT now(),`,
            `  idempotency_key text,`,
            `  attempted_at timestamptz,`,
            `  attempted_by text[] NOT NULL DEFAULT ARRAY[]::text[],`,
            `  completed_at timestamptz,`,
            `  cancelled_at timestamptz,`,
            `  discarded_at timestamptz,`,
            `  errors jsonb NOT NULL DEFAULT '[]'::jsonb,`,
            `  inserted_at timestamptz NOT NULL DEFAULT now(),`,
            `  updated_at timestamptz NOT NULL DEFAULT now(),`,
            `  CONSTRAINT ${indexName(options, "status_check")} CHECK (status IN (${statusList})),`,
            `  CONSTRAINT ${indexName(options, "attempt_check")} CHECK (attempt >= 0 AND executions >= 0 AND snoozes >= 0 AND max_attempts >= 1)`,
            `);`,
            `CREATE INDEX IF NOT EXISTS ${readyIndex} ON ${table} (queue, priority, run_at, inserted_at) WHERE status IN ('available', 'scheduled', 'retryable');`,
            `CREATE INDEX IF NOT EXISTS ${statusQueueIndex} ON ${table} (status, queue);`,
            `CREATE INDEX IF NOT EXISTS ${executingIndex} ON ${table} (attempted_at) WHERE status = 'executing';`,
            `CREATE UNIQUE INDEX IF NOT EXISTS ${idempotencyIndex} ON ${table} (name, queue, idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN (${activeStatusList});`,
        ].join("\n"),
        down: `DROP TABLE IF EXISTS ${table};`,
    };
};
