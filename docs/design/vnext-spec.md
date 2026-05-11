# Effect Job vNext Specification

Status: draft design target

This document records the product decisions, API direction, runtime architecture, data model, integration points, dashboard primitives, and implementation roadmap for the full redesign of Effect Job.

The current codebase is a useful prototype. vNext should be treated as a clean redesign.

## 1. Product Vision

Effect Job should become the best open-source, Effect-native job library for the JavaScript and TypeScript ecosystem.

The design is inspired by Oban and Oban Pro, but it should not clone their API. We should copy the durable state machine, production lessons, and operational maturity, while using Effect to make jobs typed, composable, observable, and extensible.

The core promise:

- Start simple.
- Stay type-safe.
- Scale to serious production workloads.
- Explain what the system is doing.
- Integrate with the rest of the user's stack.
- Keep the runtime open source and trusted.

## 2. Product Split

The recommended product split is:

- Core runtime: open source, free forever, no artificial job limits.
- Dashboard/control plane: commercial or source-available, free up to a generous usage threshold.
- Optional adapters: separate packages over time.

The core runtime must never stop job execution because of dashboard billing.

Suggested future package shape:

```txt
effect-job                 # current repo/package during redesign
@effect/jobs               # possible future core package
@effect/jobs-dashboard     # future dashboard/control plane
@effect/jobs-drizzle       # future Drizzle helpers if split
@effect/jobs-redis         # optional notifier/rate-limit adapter
@effect/jobs-nats          # optional notifier adapter
@effect/jobs-s3            # optional archive adapter
@effect/jobs-opentelemetry # optional telemetry adapter if split
@effect/jobs-cluster       # future Effect Cluster adapter
@effect/jobs-workflow      # future Effect Workflow adapter
```

During early development, one package is acceptable. Internally, keep boundaries clean so packages can split later.

## 3. Core Goals

- Make Effect first-class in every runtime operation.
- Use Effect Schema for typed payloads, results, and dashboard-safe projections.
- Keep the beginner API small.
- Make advanced behavior configurable through policies and hooks.
- Provide one excellent Postgres runtime first.
- Support Drizzle first, without making the product a Drizzle-only queue.
- Design storage boundaries for other databases later.
- Design integration ports for external services.
- Support Oban Pro-inspired dynamic runtime features.
- Keep workflow support as interop with Effect Workflow, not an in-library workflow engine.
- Prepare for Effect Cluster integration without building a custom distributed worker protocol.
- Make dashboard and observability primitives part of the core data model.

## 4. Non-Goals

- Do not build a workflow engine.
- Do not support MySQL, SQLite, Turso, DynamoDB, Redis-backed jobs, or SQS in v1.
- Do not expose multiple public engines in the first API.
- Do not make users choose an engine for normal use.
- Do not expose Ecto-style changesets.
- Do not make Drizzle a hard requirement for application code.
- Do not require advanced configuration for simple jobs.
- Do not paywall the ability to run jobs, retry jobs, schedule jobs, or inspect basic job state.

## 5. Decision Log

### 5.1 Runtime-Independent Job Definitions

Job definitions should not be created from a configured database runtime.

Use this:

```ts
const Catalog = JobCatalog.define({
  queues: {
    default: Queue.define(),
    mailers: Queue.define(),
  },
})

const SendEmail = Catalog.job({
  name: "email.send",
  queue: "mailers",
  payload: SendEmailPayload,
  run,
})

const Jobs = JobSystem.postgres({
  url: process.env.DATABASE_URL!,
  catalog: Catalog,
  jobs: [SendEmail],
})
```

Avoid this as the primary API:

```ts
const Jobs = JobSystem.postgres(...)
const SendEmail = Jobs.job(...)
```

Why:

- Job definitions can live in shared packages.
- Producers and workers can import the same definitions.
- Tests can use the same definitions with manual or inline runtimes.
- Runtime config can differ by environment without redefining jobs.

### 5.2 Type-Safe Queues Through Catalogs

Queues should be type-safe. The catalog defines known queues, and jobs can only use those queues unless they explicitly opt into dynamic queues.

```ts
const Catalog = JobCatalog.define({
  queues: {
    default: Queue.define(),
    mailers: Queue.define(),
    webhooks: Queue.define(),
    media: Queue.define(),
  },
})

const SendEmail = Catalog.job({
  name: "email.send",
  queue: "mailers",
  payload,
  run,
})
```

Typos should fail at compile time:

```ts
Catalog.job({
  name: "email.send",
  queue: "mailer",
  payload,
  run,
})
```

Dynamic queue policies can still be type-safe:

```ts
const NotifyUser = Catalog.job({
  name: "user.notify",
  queue: ({ payload }) => payload.urgent ? "webhooks" : "mailers",
  payload: NotifyPayload,
  run,
})
```

Truly dynamic queues require an explicit escape hatch:

```ts
const TenantJob = Catalog.job({
  name: "tenant.work",
  queue: ({ payload }) => Queue.dynamic(`tenant:${payload.tenantId}`),
  payload: TenantPayload,
  run,
})
```

### 5.3 `enqueue` Is The Happy Path

The common API should be:

```ts
yield* SendEmail.enqueue(payload, options)
```

This means "validate this payload, resolve options, insert a durable job, notify workers, and return a handle."

### 5.4 Keep The Two-Step Command Path

Oban has `Worker.new(...) |> Oban.insert(...)` because it builds an Ecto changeset and then persists it.

For Effect Job, keep the same power without the ceremony:

```ts
const command = SendEmail.command(payload, options)
yield* Jobs.insert(command)
```

`command` should be pure. `insert` should be Effectful.

`enqueue` should be sugar for command plus insert:

```ts
const enqueue = (payload, options) =>
  SendEmail.command(payload, options).pipe(Jobs.insert)
```

The command path enables:

- Transactional enqueue.
- Mixed job bulk insert.
- Command inspection.
- Command transformation.
- Cron generation.
- Workflow adapters.
- Tests.

### 5.5 Use `command`, Not `make`

Use:

```ts
SendEmail.command(payload)
```

Avoid:

```ts
SendEmail.make(payload)
```

Reason: `make` is vague and can be confused with job definition. `command` clearly means "build a typed insert command."

### 5.6 Postgres First

The first production backend is Postgres only.

Reasons:

- `FOR UPDATE SKIP LOCKED`.
- JSONB.
- Partial indexes.
- Advisory locks.
- LISTEN/NOTIFY.
- Strong transactions.
- Upserts.
- Partitioning.
- Mature operational ecosystem.

Other databases can come later behind an internal storage boundary.

### 5.7 Drizzle First, But Not Drizzle-Only

Drizzle should be the first TypeScript database integration because it is TypeScript-native, close to SQL, supports Postgres well, and has a migration story.

But the product should not be "a Drizzle job queue."

The public stance:

```ts
JobSystem.postgres(...)
```

Drizzle integration is an option/helper:

```ts
JobSystem.postgres({
  drizzle: db,
})
```

Hot-path runtime operations may use optimized SQL through Drizzle's SQL escape hatch or Effect SQL. Drizzle should help with schema, migrations, transaction interop, and developer ergonomics, but it must not limit the queue engine.

### 5.8 Memory Is For Tests, Not A Production Backend

Keep manual/inline/memory behavior for tests. Do not present memory as an equal production backend.

### 5.9 Support Runtime Roles

Real deployments often split producers and workers.

Support roles:

```ts
role: "producer" | "worker" | "all"
```

Examples:

```ts
const ProducerJobs = JobSystem.postgres({
  url,
  catalog: Catalog,
  role: "producer",
})
```

```ts
const WorkerJobs = JobSystem.postgres({
  url,
  catalog: Catalog,
  jobs: [SendEmail, DeliverWebhook],
  role: "worker",
  queues: {
    mailers: { concurrency: 20 },
    webhooks: { concurrency: 50 },
  },
})
```

### 5.10 Dashboard Primitives Belong In Core

Even if the dashboard is commercial later, the core runtime should emit the data needed for an excellent dashboard:

- Job events.
- Public payload projections.
- Dashboard dimensions.
- Job timeline.
- Queue health.
- Node health.
- Cluster health placeholders.
- Blocked reasons.
- Usage aggregation.
- Realtime subscriptions.
- Auth hooks.
- Admin action hooks.

## 6. API Overview

### 6.1 Minimal Program

```ts
import { Effect, Schema } from "effect"
import { JobCatalog, JobSystem, Queue } from "effect-job"

const Catalog = JobCatalog.define({
  queues: {
    default: Queue.define(),
  },
})

const SendEmail = Catalog.job({
  name: "email.send",
  payload: Schema.Struct({
    userId: Schema.String,
    email: Schema.String,
  }),
  run: ({ payload }) =>
    Effect.gen(function* () {
      const email = yield* EmailService
      yield* email.send(payload.userId, payload.email)
    }),
})

const Jobs = JobSystem.postgres({
  url: process.env.DATABASE_URL!,
  catalog: Catalog,
  jobs: [SendEmail],
})

yield* SendEmail.enqueue({
  userId: "user_123",
  email: "person@example.com",
})
```

The catalog can include an implicit `default` queue for the simplest setup, but explicit queues should be encouraged for production.

### 6.2 Production Setup

```ts
const Catalog = JobCatalog.define({
  queues: {
    default: Queue.define(),
    mailers: Queue.define(),
    webhooks: Queue.define(),
    media: Queue.define(),
  },
})

const Jobs = JobSystem.postgres({
  url: process.env.DATABASE_URL!,
  catalog: Catalog,
  jobs: [SendEmail, DeliverWebhook, ResizeImage],
  role: "all",
  preset: "balanced",
  queues: {
    default: { concurrency: 10 },
    mailers: { concurrency: 25 },
    webhooks: {
      concurrency: 50,
      globalConcurrency: 500,
      rateLimit: {
        limit: 100,
        per: "10 seconds",
        key: ({ job }) => ["account", job.payload.accountId],
      },
    },
    media: { concurrency: 2 },
  },
  defaults: {
    attempts: 20,
    timeout: "5 minutes",
  },
})
```

### 6.3 Job Definition

```ts
const DeliverWebhook = Catalog.job({
  name: "webhook.deliver",
  queue: "webhooks",
  payload: Schema.Struct({
    accountId: Schema.String,
    endpointId: Schema.String,
    eventId: Schema.String,
    url: Schema.String,
    body: Schema.Unknown,
  }),
  result: Schema.Struct({
    status: Schema.Number,
    durationMs: Schema.Number,
  }),
  attempts: {
    max: 20,
    backoff: ({ error }) => {
      if (error instanceof RateLimitError) return "10 minutes"
      return "30 seconds"
    },
    classify: ({ error }) => {
      if (error instanceof PermanentWebhookError) return "discard"
      return "retry"
    },
  },
  timeout: "15 seconds",
  unique: {
    key: ({ payload }) => ["webhook", payload.endpointId, payload.eventId],
    while: ["scheduled", "available", "executing", "retryable"],
    for: "forever",
  },
  concurrency: {
    key: ({ payload }) => ["endpoint", payload.endpointId],
    limit: 1,
    scope: "global",
  },
  rateLimit: {
    key: ({ payload }) => ["account", payload.accountId],
    limit: 100,
    per: "10 seconds",
    scope: "global",
  },
  dashboard: {
    title: ({ payload }) => `Webhook ${payload.eventId}`,
    dimensions: {
      account: ({ payload }) => payload.accountId,
      endpoint: ({ payload }) => payload.endpointId,
    },
    publicPayload: ({ payload }) => ({
      accountId: payload.accountId,
      endpointId: payload.endpointId,
      eventId: payload.eventId,
      url: payload.url,
    }),
  },
  run: ({ payload }) =>
    Effect.gen(function* () {
      const http = yield* HttpClient
      const startedAt = Date.now()
      const response = yield* http.post(payload.url, { json: payload.body })

      if (response.status === 429) {
        return yield* Jobs.snooze("10 minutes", "Endpoint rate limited")
      }

      if (response.status >= 400 && response.status < 500) {
        return yield* Jobs.discard(`Permanent HTTP ${response.status}`)
      }

      return {
        status: response.status,
        durationMs: Date.now() - startedAt,
      }
    }),
})
```

### 6.4 Command And Enqueue API

Convenience:

```ts
yield* SendEmail.enqueue(payload, options)
```

Advanced command path:

```ts
const command = SendEmail.command(payload, options)
yield* Jobs.insert(command)
```

Pipeable command transformation:

```ts
yield* SendEmail
  .command(payload)
  .pipe(
    JobCommand.withMeta({ source: "admin-panel" }),
    JobCommand.withPriority(-5),
    Jobs.insert,
  )
```

Mixed bulk insert:

```ts
yield* Jobs.insertMany([
  SendEmail.command(payloadA),
  DeliverWebhook.command(payloadB),
  SyncAccount.command(payloadC),
])
```

Job-specific bulk insert:

```ts
yield* SendEmail.enqueueMany(payloads, {
  chunkSize: 1_000,
  concurrency: 4,
})
```

Stream insert:

```ts
yield* SendEmail.enqueueStream(payloadStream, {
  chunkSize: 1_000,
  onInvalidPayload: "collect-errors",
})
```

Resolve without inserting:

```ts
const command = SendEmail.command(payload, options)
const resolved = yield* Jobs.resolve(command)
```

## 7. Effect-First Design

Every runtime operation returns an `Effect`:

```ts
SendEmail.enqueue(payload): Effect.Effect<JobHandle, EnqueueError, JobRuntime | Requirements>
Jobs.insert(command): Effect.Effect<JobHandle, InsertError, JobRuntime>
Jobs.run: Effect.Effect<never, never, JobRuntime | Scope.Scope>
```

Promise helpers may exist at boundaries:

```ts
await Jobs.runPromise(SendEmail.enqueue(payload))
```

But Promise helpers are convenience, not the primary API.

Callbacks may return plain values or Effects.

```ts
attempts: {
  max: ({ payload }) =>
    Effect.gen(function* () {
      const billing = yield* BillingPlanService
      const plan = yield* billing.getPlan(payload.accountId)
      return plan.enterprise ? 50 : 10
    }),
}
```

## 8. Policy Model

Most configurable behavior should be represented as a policy:

```ts
type Policy<Context, Value, Requirements = never> =
  | Value
  | ((context: Context) => Value)
  | ((context: Context) => Effect.Effect<Value, never, Requirements>)
```

Policies should apply to:

- Queue selection.
- Priority.
- Tags.
- Attempts max.
- Backoff.
- Error classification.
- Timeout.
- Unique key.
- Concurrency key.
- Concurrency limit.
- Rate-limit key.
- Rate-limit weight.
- Dashboard title.
- Dashboard dimensions.
- Public payload projection.
- Archive routing.
- Explain/blocked reason custom logic.

Examples:

```ts
attempts: 5
```

```ts
attempts: {
  max: ({ payload }) => payload.vip ? 30 : 10,
}
```

```ts
attempts: {
  max: ({ payload }) =>
    Effect.gen(function* () {
      const plans = yield* BillingPlanService
      const plan = yield* plans.get(payload.accountId)
      return plan.enterprise ? 50 : 10
    }),
}
```

## 9. Job States And Outcomes

### 9.1 States

Core states:

```ts
type JobState =
  | "scheduled"
  | "available"
  | "executing"
  | "retryable"
  | "completed"
  | "cancelled"
  | "discarded"
  | "suspended"
```

`suspended` should only exist if public queue/job suspend behavior is implemented.

### 9.2 Outcome Semantics

Normal Effect success completes the job.

```ts
run: () => Effect.void
```

Effect failure retries or discards according to classification and attempt budget.

```ts
run: () => Effect.fail(new TemporaryError())
```

Explicit helpers:

```ts
return yield* Jobs.snooze("10 minutes", "External system not ready")
return yield* Jobs.cancel("No longer needed")
return yield* Jobs.discard("Permanent failure")
```

### 9.3 Attempts, Executions, Snoozes

Track these separately:

```ts
attempts: number     // failure attempts that consume retry budget
executions: number   // total worker starts
snoozes: number      // intentional deferrals
```

Snooze should not consume retry attempts by default.

## 10. Separate Concepts

The API must keep these separate:

- `unique`: should another job row be inserted?
- `concurrency`: may matching jobs run at the same time?
- `rateLimit`: may matching jobs start now?
- `idempotency`: should the side effect run again or reuse an earlier result?
- `ordering`: must this job wait behind a related job?

Do not merge these into one overloaded feature.

## 11. Oban Pro-Inspired Dynamic Features

We should learn from Oban Pro's dynamic features and support the same category of production needs in an open-source-friendly way.

### 11.1 Dynamic Queues

Static queues come from the catalog and runtime config. Dynamic queues are stored in the database and can be changed at runtime.

Use cases:

- Pause/resume queues without deploys.
- Scale concurrency without deploys.
- Create tenant-specific queues.
- Disable a noisy queue during incidents.
- Dynamically route work to new queues.

API direction:

```ts
yield* Jobs.queues.create({
  name: Queue.dynamic("tenant:tenant_123"),
  concurrency: 5,
  globalConcurrency: 20,
})

yield* Jobs.queues.pause("webhooks")
yield* Jobs.queues.resume("webhooks")
yield* Jobs.queues.scale("webhooks", { concurrency: 100 })
yield* Jobs.queues.scaleGlobal("webhooks", { concurrency: 1_000 })
```

Dynamic queue config should be versioned and audited.

### 11.2 Dynamic Schedules

Cron/scheduled recurring jobs should support static definitions and dynamic DB-backed definitions.

Static:

```ts
const Schedules = Catalog.schedules({
  nightlyCleanup: {
    cron: "0 0 * * *",
    timezone: "Etc/UTC",
    job: NightlyCleanup,
    payload: undefined,
    unique: { key: "per-tick" },
  },
})
```

Dynamic:

```ts
yield* Jobs.schedules.upsert({
  id: "tenant_123.hourly-sync",
  cron: "0 * * * *",
  timezone: "Etc/UTC",
  job: SyncTenant,
  payload: { tenantId: "tenant_123" },
  enabled: true,
})
```

Recurring jobs should be inserted by the leader only, with per-tick uniqueness.

### 11.3 Dynamic Limits

Runtime limits should be mutable without deploys:

- Queue local concurrency.
- Queue global concurrency.
- Partitioned concurrency.
- Rate limits.
- Rate-limit weights.
- Dispatch cooldown.
- Pause/resume.

Dynamic limit changes should affect new dispatch decisions quickly and should be visible in diagnostics.

### 11.4 Dynamic Maintenance

Maintenance settings may become dynamic later:

- Prune intervals.
- Retention windows.
- Rescue/lifeline windows.
- Archive destinations.

Early versions can keep these static, but the schema should not block dynamic control later.

### 11.5 Dynamic Config Architecture

Use a database-backed control table for dynamic config:

```txt
effect_job_dynamic_config
effect_job_queue_config
effect_job_schedule_config
effect_job_config_events
```

Requirements:

- Version every dynamic config change.
- Record actor/source.
- Broadcast updates to workers.
- Fall back to polling if notifications fail.
- Validate dynamic config against catalog and runtime capabilities.
- Make config changes explainable in the dashboard.

## 12. Runtime Configuration

### 12.1 Presets

Provide presets to reduce initial config burden:

```ts
preset: "simple" | "balanced" | "throughput" | "strict"
```

Suggested meanings:

- `simple`: low ceremony, local-friendly defaults.
- `balanced`: production-friendly defaults.
- `throughput`: batching and DB-pressure-aware defaults.
- `strict`: conservative timeouts, stronger validation, safer retry behavior.

### 12.2 Postgres Config

```ts
const Jobs = JobSystem.postgres({
  url: process.env.DATABASE_URL!,
  schema: "public",
  tablePrefix: "effect_jobs",
  migrate: "safe",
  poolSize: 10,
  statementTimeout: "15 seconds",
})
```

### 12.3 Drizzle Config

```ts
const Jobs = JobSystem.postgres({
  drizzle: db,
  catalog: Catalog,
})
```

Drizzle transaction interop:

```ts
yield* Jobs.withDrizzleTransaction(tx,
  Effect.gen(function* () {
    yield* SendEmail.enqueue(payload)
  }),
)
```

Effect-native transaction helper:

```ts
yield* Jobs.transaction(
  Effect.gen(function* () {
    const user = yield* Users.create(input)
    yield* SendEmail.enqueue({ userId: user.id, email: user.email })
  }),
)
```

### 12.4 Acknowledgements

Support batched acknowledgement from the beginning, even if initial implementation starts simple.

```ts
acknowledgements: {
  mode: "sync" | "batched",
  flushEvery: "50 millis",
  maxBatchSize: 500,
  flushOnShutdown: true,
}
```

### 12.5 Inserts

```ts
inserts: {
  mode: "sync" | "batched",
  maxBatchSize: 1_000,
  notify: "none" | "postgres" | "coalesced",
}
```

## 13. Storage And Postgres Schema Direction

The first production storage is Postgres.

Do not make the public API expose multiple engines, but keep an internal storage boundary:

```ts
interface JobStorage {
  insert(command: ResolvedJobCommand): Effect.Effect<JobRecord, JobStorageError>
  insertMany(commands: ReadonlyArray<ResolvedJobCommand>): Effect.Effect<ReadonlyArray<JobRecord>, JobStorageError>
  fetchAvailable(input: FetchInput): Effect.Effect<ReadonlyArray<JobRecord>, JobStorageError>
  acknowledge(input: AcknowledgeInput): Effect.Effect<void, JobStorageError>
  stageDueJobs(input: StageInput): Effect.Effect<number, JobStorageError>
  rescueOrphans(input: RescueInput): Effect.Effect<number, JobStorageError>
  prune(input: PruneInput): Effect.Effect<number, JobStorageError>
  explainJob(input: ExplainInput): Effect.Effect<JobExplanation, JobStorageError>
}
```

### 13.1 Initial Tables

Likely tables:

```txt
effect_jobs_jobs
effect_jobs_events
effect_jobs_results
effect_jobs_nodes
effect_jobs_queues
effect_jobs_schedules
effect_jobs_unique_keys
effect_jobs_rate_limits
effect_jobs_usage_daily
effect_jobs_dynamic_config
```

### 13.2 Jobs Table Direction

Important columns:

```txt
id
name
queue
state
payload
public_payload
result
meta
tags
priority
attempts
executions
snoozes
max_attempts
scheduled_at
attempted_at
completed_at
cancelled_at
discarded_at
inserted_at
updated_at
node_id
unique_key
concurrency_key
rate_limit_key
last_error_class
last_error_message
blocked_reason
```

### 13.3 Events Table Direction

Append-only events support timelines, audit, analytics, realtime, dashboard, and debugging.

Important fields:

```txt
id
job_id
event_type
at
node_id
queue
name
state_from
state_to
data
```

### 13.4 Usage Table Direction

Track dashboard billing/usage locally:

```txt
date
job_count
completed_count
failed_count
discarded_count
dashboard_user_count
storage_bytes_estimate
```

Billing must never block job execution.

### 13.5 Fetch Query Direction

The core safe Postgres pattern is `FOR UPDATE SKIP LOCKED`:

```sql
WITH selected AS (
  SELECT id
  FROM effect_jobs_jobs
  WHERE queue = $1
    AND state = 'available'
    AND scheduled_at <= now()
  ORDER BY priority ASC, scheduled_at ASC, id ASC
  LIMIT $2
  FOR UPDATE SKIP LOCKED
)
UPDATE effect_jobs_jobs
SET state = 'executing',
    executions = executions + 1,
    attempted_at = now(),
    updated_at = now(),
    node_id = $3
WHERE id IN (SELECT id FROM selected)
RETURNING *;
```

This will become more complex with global concurrency, partitioned concurrency, rate limits, and fairness.

## 14. Diagnostics And Explainability

Diagnostics should be a flagship feature.

Core APIs:

```ts
yield* Jobs.explain.job(jobId)
yield* Jobs.explain.queue("webhooks")
yield* Jobs.explain.system()
```

Job explanation example:

```ts
{
  state: "available",
  canRunNow: false,
  reasons: [
    {
      type: "GlobalConcurrencyFull",
      queue: "webhooks",
      running: 500,
      limit: 500,
    },
    {
      type: "RateLimited",
      key: ["account", "account_123"],
      availableAt: new Date("2026-05-11T12:00:10.000Z"),
    },
  ],
}
```

Blocked reason categories:

- Queue paused.
- Queue missing.
- Worker missing.
- No local capacity.
- Global concurrency full.
- Partition concurrency full.
- Rate limited.
- Scheduled for the future.
- Waiting for leadership.
- Node ownership stale.
- Dynamic config disabled.
- Custom extension reason.

Custom explain providers:

```ts
const TenantSuspension = Jobs.extension({
  name: "tenant-suspension",
  explainBlockedReason: ({ job }) =>
    Effect.gen(function* () {
      const tenants = yield* TenantService
      const suspended = yield* tenants.isSuspended(job.meta.tenantId)
      return suspended ? [{ type: "TenantSuspended" }] : []
    }),
})
```

## 15. Dashboard And Control Plane Primitives

The paid dashboard should be possible because the core runtime emits rich primitives.

### 15.1 Core Dashboard Data

The core should provide:

- Job search/read APIs.
- Job timeline APIs.
- Queue health APIs.
- Node/cluster health APIs.
- Realtime event streams.
- Public payload projection.
- Dashboard dimensions.
- Usage aggregation.
- Admin action hooks.
- Authorization hooks.

### 15.2 Public Payload Projection

Never assume raw payload is safe for dashboard display.

```ts
dashboard: {
  publicPayload: ({ payload }) => ({
    accountId: payload.accountId,
    endpointId: payload.endpointId,
    eventId: payload.eventId,
  }),
}
```

### 15.3 Dimensions

Dimensions enable tenant/account/customer analytics.

```ts
dashboard: {
  dimensions: {
    tenant: ({ payload }) => payload.tenantId,
    account: ({ payload }) => payload.accountId,
  },
}
```

### 15.4 Dashboard Auth

Hosted-in-app dashboard needs user auth hooks.

```ts
dashboard: {
  auth: {
    canViewJob: ({ user, job }) =>
      user.role === "admin" || job.dashboardDimensions.tenant === user.tenantId,
    canRetryJob: ({ user }) => user.role === "admin",
    canCancelJob: ({ user }) => user.role === "admin",
  },
}
```

### 15.5 Dashboard Actions

User-defined actions make the dashboard customizable.

```ts
dashboard: {
  actions: [
    {
      id: "replay-webhook",
      label: "Replay webhook",
      visible: ({ job }) => job.name === "webhook.deliver",
      run: ({ job }) => DeliverWebhook.enqueue(job.payload),
    },
  ],
}
```

### 15.6 Search Strategy

Start with Postgres indexes and JSONB projections.

Later adapters:

- ParadeDB for Postgres-native advanced search.
- Typesense for external instant search.
- ClickHouse for analytics-heavy workloads.

Do not require these in v1.

### 15.7 Dashboard Licensing

Recommended commercial model:

- Core runtime: MIT/Apache, no job limits.
- Dashboard: free up to 1M processed jobs per month.
- Paid above threshold.
- Soft enforcement.
- Job execution never blocked.

## 16. Integrations And Ports

Do not hardcode every service. Define ports and adapters.

Core ports:

```txt
JobStorage        # Postgres hot table and transactions
JobNotifier       # wake workers
JobExecution      # local or cluster execution
JobArchive        # cold storage/data warehouse
JobTelemetry      # metrics/tracing/events
JobSearch         # dashboard search
JobAnalytics      # dashboard analytics
JobRealtime       # dashboard realtime stream
JobRateLimitStore # Redis/Postgres/custom quota store
JobLockStore      # advisory/distributed locks
JobSerializer     # JSON/encryption/compression
DashboardAuth     # hosted dashboard authorization
DashboardActions  # custom dashboard operations
JobWorkflowBridge # Effect Workflow interop
```

### 16.1 Notifier

Postgres remains source of truth. Notifiers only wake workers.

```ts
notifier: JobNotifier.race([
  JobNotifier.postgresListenNotify(),
  JobNotifier.polling({ every: "500 millis" }),
])
```

Future adapters:

- Redis Pub/Sub.
- NATS.
- Kafka.
- Custom.

### 16.2 Execution

Default local execution:

```ts
execution: JobExecution.local()
```

Future Effect Cluster execution:

```ts
execution: JobExecution.cluster({
  entityType: "job-worker",
  route: ({ job }) => ({
    entityId: job.queue,
    shardKey: job.queue,
  }),
})
```

### 16.3 Archive

```ts
archive: JobArchive.s3({
  bucket: "job-archives",
  prefix: "jobs/",
  format: "jsonl",
})
```

### 16.4 Telemetry

Emit standard telemetry and lifecycle events.

```ts
telemetry: [
  JobTelemetry.effectMetrics(),
  JobTelemetry.openTelemetry(),
]
```

## 17. Effect Cluster Integration

Effect Cluster should be an execution placement adapter, not a storage backend.

The job system remains responsible for:

- Durable state.
- Policies.
- Scheduling.
- Retry semantics.
- Queue diagnostics.
- Dashboard data.

Effect Cluster can later handle:

- Distributed worker placement.
- Sharding/routing.
- Runner health.
- Cluster coordination.

Design now so local execution and cluster execution use the same job definition and storage model.

## 18. Effect Workflow Integration

Do not implement workflow dependencies, DAGs, or long-running workflow semantics.

Expose adapters only.

Job as workflow activity:

```ts
const SendEmailActivity = SendEmail.asWorkflowActivity({
  mode: "enqueue-and-await",
  result: "typed",
})
```

Workflow starter as job:

```ts
const StartOnboardingWorkflow = Catalog.fromWorkflow({
  name: "workflow.onboarding.start",
  workflow: OnboardingWorkflow,
  payload: OnboardingPayload,
  executionId: ({ payload }) => payload.userId,
})
```

Boundary:

- Job system: durable task queue, scheduling, retries, limits, diagnostics.
- Workflow system: orchestration, dependencies, long-running business process.

## 19. Extensions, Hooks, Middleware

### 19.1 Extensions

Extensions are named groups of hooks and policy overrides.

```ts
const TenantExtension = Jobs.extension({
  name: "tenant-context",
  beforeInsert: ({ job }) =>
    Effect.gen(function* () {
      const tenant = yield* TenantContext
      return job.withMeta({ tenantId: tenant.id })
    }),
  classifyError: ({ error, fallback }) => {
    if (error instanceof TenantDisabledError) return "cancel"
    return fallback(error)
  },
})
```

### 19.2 Job Hooks

```ts
hooks: {
  beforeEnqueue: ({ payload, options }) => Effect.succeed({ payload, options }),
  beforeRun: ({ job }) => Effect.logInfo("job started", { jobId: job.id }),
  afterComplete: ({ job, result }) => Effect.logInfo("job completed", { jobId: job.id }),
  afterDiscard: ({ job, reason }) => Alerts.notify(reason),
}
```

### 19.3 Middleware

```ts
Jobs.use([
  Jobs.middleware.logging(),
  Jobs.middleware.tracing(),
  Jobs.middleware.make({
    name: "audit",
    around: (job, run) =>
      Effect.gen(function* () {
        yield* Audit.log("job.started", { jobId: job.id })
        const result = yield* run
        yield* Audit.log("job.finished", { jobId: job.id })
        return result
      }),
  }),
])
```

## 20. Testing API

Manual mode:

```ts
const TestJobs = Jobs.test({ mode: "manual" })

yield* App.signup(input)

yield* TestJobs.expectEnqueued(SendEmail, {
  payload: { email: "person@example.com" },
})
```

Inline mode:

```ts
const TestJobs = Jobs.test({ mode: "inline" })
```

Drain mode:

```ts
yield* TestJobs.drainQueue("mailers", {
  mode: "until-empty",
  maxJobs: 100,
})
```

Testing should not require Postgres for unit tests. Postgres integration tests should exist separately.

## 21. Runtime Controls

```ts
yield* Jobs.queues.pause("webhooks")
yield* Jobs.queues.resume("webhooks")
yield* Jobs.queues.scale("webhooks", { concurrency: 100 })
yield* Jobs.queues.scaleGlobal("webhooks", { concurrency: 1_000 })

yield* Jobs.jobs.retry(jobId)
yield* Jobs.jobs.cancel(jobId, { reason: "User requested cancellation" })
yield* Jobs.jobs.cancelWhere({ name: "webhook.deliver", state: ["scheduled"] })
yield* Jobs.jobs.snooze(jobId, { until: new Date(Date.now() + 60_000) })
```

## 22. Migration From Current Prototype

Current prototype concepts map roughly as follows:

```txt
Job.make                 -> Catalog.job
job.new                  -> job.command
Job.insert               -> Jobs.insert
effectJob(config)        -> JobSystem.postgres(config)
Worker.run               -> Jobs.run / worker runtime
JobEngine                -> internal JobStorage
JobRegistry              -> catalog + runtime job registry
plugins                  -> extensions / maintenance / dynamic features
memory()                 -> test runtime only
postgres()               -> Postgres runtime implementation
```

Breaking changes are acceptable. Do not preserve the old API unless it is nearly free.

## 23. Implementation Order

Recommended order:

1. Domain model.
2. `JobCatalog.define` and type-safe queues.
3. `Catalog.job` definitions.
4. `Job.command`, `Job.enqueue`, `Jobs.insert` API skeleton.
5. Policy resolver.
6. Manual/inline test runtime.
7. Postgres schema and migrations.
8. Postgres insert/list/get.
9. Worker fetch using `FOR UPDATE SKIP LOCKED`.
10. Execute/complete/fail/retry/discard/cancel/snooze.
11. Events table.
12. Basic diagnostics.
13. Queue controls.
14. Dynamic queue/schedule foundations.
15. Dashboard projections and query APIs.
16. Batched acknowledgements.
17. Global concurrency/rate limits.
18. Advanced dashboard/search/analytics adapters.

## 24. Initial Milestones

### Milestone 1: API Skeleton

- `JobCatalog.define`.
- `Queue.define`.
- `Catalog.job`.
- `Job.command`.
- `Job.enqueue`.
- `JobSystem.postgres` type shell.
- `Jobs.insert` API shell.
- Manual test runtime.

### Milestone 2: Basic Postgres Runtime

- Schema/migration generator.
- Insert job.
- Get/list jobs.
- Fetch available jobs.
- Execute workers.
- Complete/fail/retry/discard/cancel/snooze.
- Basic tests.

### Milestone 3: Production Semantics

- Scheduled jobs.
- Staging.
- Rescue orphaned jobs.
- Pruning/retention.
- LISTEN/NOTIFY plus polling fallback.
- Queue pause/resume.
- Diagnostics.

### Milestone 4: Advanced Runtime

- Global concurrency.
- Partitioned concurrency.
- Rate limits.
- Batched acknowledgements.
- Dynamic queues.
- Dynamic schedules.

### Milestone 5: Dashboard Foundation

- Events.
- Public payload projections.
- Dimensions.
- Usage aggregation.
- Job timeline.
- Queue health.
- Realtime stream.

## 25. Open Questions

- Should `JobCatalog.define()` include `default` automatically?
- Should `Catalog.job` require `queue`, or default to `default`?
- What should the exact `JobCommand` type expose publicly?
- Should `Jobs.resolve(command)` validate schema immediately or also resolve all policies?
- Should dashboard basic UI be open source while advanced dashboard is commercial?
- Which license should the core use: MIT or Apache-2.0?
- Should Drizzle schema helpers live in core initially or a separate package from day one?
- How much dynamic queue support should land before v1?
- Should global concurrency/rate limits be v1 or v1.x?

## 26. Design Principle

Start simple. Override anything.

Every default should be implemented as a replaceable policy. Every important decision should receive context. Every extension should be an Effect. Every blocked job should be explainable.
