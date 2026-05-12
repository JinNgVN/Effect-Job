import { PgClient } from "@effect/sql-pg";
import { Effect, Layer, Option, Redacted, Schema } from "effect";


import { effectJob } from "./effectJob";
import { Job, JobCommand } from "./job";
import { JobNotifierMemory } from "./notifier";
import { JobPluginsLive } from "./plugin";
import { pruner, rescuer } from "./plugins";
import { Queue } from "./queue";
import { memory } from "./databases/memory";
import { JobEnginePostgres } from "./databases/postgres";

export * from "./databases/memory";
export * from "./databases/postgres";
export * from "./databases/postgresSchema";
export * from "./db/schema";
export * from "./effectJob";
export * from "./engine";
export * from "./job";
export * from "./model";
export * from "./notifier";
export * from "./plugin";
export * from "./plugins";
export * from "./queue";
export * from "./registry";
export * from "./system";
export * from "./worker";


// ---------------------------------------------------------------------------
// Demo walkthrough
// ---------------------------------------------------------------------------
//
// This file is both the public package entrypoint and a runnable teaching demo.
// Importing the package only exports the library. Running this file directly
// (`bun src/index.ts`) walks through the API we want:
//
//   1. Configure one runtime with `effectJob({ ... })`.
//   2. Define job contracts with `Jobs.define(...)`.
//   3. Register worker handlers with `Job.toLayer(...)`.
//   4. Enqueue work with `Job.enqueue(...)` or `Job.command(...).pipe(Jobs.insert)`.
//   5. Run workers with `Jobs.run`.
//
// Some config below is aspirational and documented in the spec. The current
// prototype stores those fields but does not enforce every advanced behavior
// yet. That is intentional: the demo shows the shape of the dream library.

const isMain = typeof Bun !== "undefined" && import.meta.path === Bun.main;

// Production scenario:
// A real app would normally use Postgres as the durable source of truth. The
// runtime owns database access, queue execution policy, lifecycle plugins, and
// worker loops. This example is wrapped in a function so the demo can show the
// production config without opening a database connection when this file is
// imported or when the memory demo runs.
const makeProductionRuntimeExample = (url: string) => {
    const Jobs = effectJob({
        // Queue config describes execution lanes, not job behavior. This is
        // where shared capacity belongs: concurrency, polling, future global
        // limits, partitioned limits, rate limits, and batched acknowledgements.
        queues: {
            default: { concurrency: 10, pollInterval: "500 millis" },
            mailers: { concurrency: 25, pollInterval: "250 millis" },
            webhooks: {
                concurrency: 50,
                pollInterval: "100 millis",
                // Future advanced runtime controls. The current prototype does
                // not enforce all of these yet, but the API is designed so they
                // can land without changing job definitions.
                globalConcurrency: 500,
                rateLimit: {
                    limit: 100,
                    per: "10 seconds",
                    key: (context: { readonly job: { readonly name: string } }) => [
                        "queue",
                        context.job.name,
                    ],
                },
            },
            media: { concurrency: 2, pollInterval: "1 second" },
        },

        // `pollInterval` is the fallback for queues that do not choose their
        // own polling cadence. A future public `node.id` can name each running
        // server/container for dashboards and orphan rescue; today worker
        // identity is generated internally unless `Jobs.worker(...)` overrides
        // it for tests.
        pollInterval: "1 second",
        shutdownGracePeriod: "15 seconds",
    });

    // Runtime services are normal Effect layers, not fields inside
    // `effectJob(...)`. Drizzle owns migrations; `@effect/sql-pg` owns the
    // Postgres client; the job library only provides job-specific SQL.
    const Live = Layer.mergeAll(
        PgClient.layer({
            url: Redacted.make(url),
        }),
        JobEnginePostgres.layer({
            schema: "public",
            table: "effect_jobs",
        }),
        // Until we add a Postgres LISTEN/NOTIFY notifier, the memory notifier
        // is a fast local wake-up path and polling remains the correctness path.
        JobNotifierMemory,
        JobPluginsLive(
            pruner({
                every: "1 hour",
                olderThan: "7 days",
            }),
            rescuer({
                every: "1 minute",
                rescueAfter: "30 minutes",
            }),
        ),
    );

    return { Jobs, Live };
};

if (isMain) {
    // Demo scenario:
    // Use the memory adapter so the walkthrough runs without Postgres. Memory
    // is for tests and demos, not production durability.
    const Jobs = effectJob({
        // This demo configures queues explicitly. The spec also allows a
        // beginner path where the runtime creates a `default` queue
        // automatically. Advanced users can later opt out with
        // `defaultQueue: false` when they want every job to choose an explicit
        // queue.
        queues: {
            default: { concurrency: 1, pollInterval: "50 millis" },
            mailers: { concurrency: 2, pollInterval: "50 millis" },
            webhooks: { concurrency: 2, pollInterval: "50 millis" },
            media: { concurrency: 1, pollInterval: "100 millis" },
        },
        shutdownGracePeriod: "2 seconds",
    });

    // Job definition scenario:
    // `Jobs.define` creates the contract for a job type. It does not contain
    // the handler. The contract is safe to import from producers and workers:
    // name, queue policy, payload/result schemas, retry policy, uniqueness, and
    // dashboard-safe projections.
    const SendEmail = Jobs.define({
        name: "email.send",

        // Queue names are type-safe from the runtime config. A typo like
        // "mailer" would fail at compile time.
        queue: "mailers",

        // Payloads are typed and encoded/decoded through Effect Schema before
        // storage and before handler execution.
        payload: Schema.Struct({
            userId: Schema.String,
            email: Schema.String,
            subject: Schema.String,
        }),

        // Results are not deeply implemented yet, but the type belongs on the
        // job contract so later relay/awaitable jobs and dashboards can be
        // typed.
        result: Schema.Struct({
            delivered: Schema.Boolean,
        }),

        // Job policy: how this job type retries after failures. This is separate
        // from queue execution capacity.
        attempts: {
            max: 5,
            backoff: ({ attempt }) => `${attempt * 100} millis`,
        },
        timeout: "10 seconds",

        // Uniqueness answers: should another row be inserted? It is not the same
        // as idempotency, which will later answer: should the side effect run
        // again or reuse a previous result?
        unique: {
            key: ({ payload }) => ["email", payload.userId, payload.subject],
            while: ["available", "scheduled", "executing", "retryable"],
        },

        // Dashboard data must be safe by construction. Raw payloads can contain
        // secrets or PII, so jobs define a public projection and dimensions for
        // search/analytics.
        dashboard: {
            title: ({ payload }) => `Email ${payload.subject}`,
            dimensions: {
                user: ({ payload }) => payload.userId,
            },
            publicPayload: ({ payload }) => ({
                userId: payload.userId,
                email: payload.email,
                subject: payload.subject,
            }),
        },
    });

    // Dynamic routing scenario:
    // Queue selection can be a policy. Urgent webhooks use the statically
    // configured `webhooks` queue. Non-urgent tenant work can opt into a dynamic
    // queue name explicitly.
    const DeliverWebhook = Jobs.define({
        name: "webhook.deliver",
        queue: ({ payload }) =>
            payload.urgent
                ? "webhooks"
                : Queue.dynamic(`tenant:${payload.tenantId}`),
        payload: Schema.Struct({
            tenantId: Schema.String,
            endpointId: Schema.String,
            eventId: Schema.String,
            urgent: Schema.Boolean,
        }),
        attempts: {
            max: ({ payload }) => (payload.urgent ? 10 : 3),
        },

        // Future advanced feature: rate-limit starts by tenant/account/API key.
        // The current prototype stores the shape; the smart runtime will enforce
        // it later.
        rateLimit: {
            key: ({ payload }) => ["tenant", payload.tenantId],
            limit: 100,
            per: "10 seconds",
            scope: "global",
        },
    });

    // Concurrency scenario:
    // Some work should never run twice for the same entity. Concurrency controls
    // execution, while uniqueness controls insertion. Keeping them separate is
    // one of the core design principles.
    const ResizeImage = Jobs.define({
        name: "media.resize",
        queue: "media",
        payload: Schema.Struct({
            imageId: Schema.String,
            width: Schema.Number,
            height: Schema.Number,
        }),
        concurrency: {
            key: ({ payload }) => ["image", payload.imageId],
            limit: 1,
            scope: "global",
        },
    });

    // Handler registration scenario:
    // `toLayer` turns a job contract into an Effect Layer that registers the
    // handler in `JobRegistry`. The worker later claims a row, looks up the
    // handler by job name, decodes the payload, and runs it.
    //
    // This is why job definitions do not use inline `run`: producers can import
    // job contracts without pulling in worker-only services.
    const WorkerLive = Layer.mergeAll(
        SendEmail.toLayer(({ payload }) =>
            Effect.gen(function* () {
                yield* Effect.logInfo(
                    `sending "${payload.subject}" to ${payload.email}`,
                );
                return { delivered: true };
            }),
        ),
        DeliverWebhook.toLayer(({ payload }) =>
            Effect.logInfo(`delivering webhook ${payload.eventId}`),
        ),
        ResizeImage.toLayer(({ payload }) =>
            Effect.gen(function* () {
                yield* Effect.logInfo(
                    `resizing ${payload.imageId} to ${payload.width}x${payload.height}`,
                );
                return yield* Jobs.snooze("100 millis", "demo resize delay");
            }),
        ),
    );
    const DemoLive = Layer.mergeAll(
        memory(),
        JobNotifierMemory,
        WorkerLive,
        JobPluginsLive({
            name: "demo-events",
            // Plugins are broad lifecycle hooks. Later, these same events
            // become dashboard timelines, metrics, and realtime updates.
            onJobEnqueued: ({ job }) =>
                Effect.logInfo(`enqueued ${job.name} (${job.queue})`),
            onJobStarted: ({ job }) =>
                Effect.logInfo(`started ${job.name} execution ${job.executions}`),
            onJobCompleted: ({ job }) =>
                Effect.logInfo(`completed ${job.name}`),
            onJobFailed: ({ job, retryAt }) =>
                Effect.logInfo(`failed ${job.name}, retrying at ${retryAt.toISOString()}`),
            onJobSnoozed: ({ job, runAt }) =>
                Effect.logInfo(`snoozed ${job.name} until ${runAt.toISOString()}`),
        }),
    );

    const program = Effect.gen(function* () {
        // Advanced enqueue scenario:
        // `command` is pure data. You can inspect or transform it before it
        // hits storage. This is the Effect version of Oban's two-step
        // `Worker.new(...) |> Oban.insert(...)` flow, without forcing that
        // ceremony on the happy path. Use this path for bulk inserts,
        // transactional enqueueing, custom validation, or shared helpers
        // that add metadata/tags consistently.
        const email = yield* SendEmail.command({
            userId: "user_1",
            email: "person@example.com",
            subject: "Welcome",
        }).pipe(
            JobCommand.withQueue("mailers"),
            JobCommand.withDelay("10 millis"),
            JobCommand.withMeta({ source: "src/index.ts demo" }),
            JobCommand.addTags(["demo", "email"]),
            JobCommand.withPriority(-5),
            Jobs.insert,
        );

        // Happy path scenario:
        // Most application code should use `enqueue`. It validates,
        // resolves policies, inserts durably, notifies workers, and returns
        // a typed handle. Per-job options are still available here for the
        // common overrides: delay/runAt, priority, metadata, tags, queue,
        // uniqueness/idempotency keys, and duplicate behavior.
        const webhook = yield* DeliverWebhook.enqueue({
            tenantId: "tenant_1",
            endpointId: "endpoint_1",
            eventId: "event_1",
            urgent: true,
        }, {
            priority: -1,
            meta: {
                requestedBy: "signup-flow",
                accountTier: "enterprise",
            },
            tags: ["demo", "webhook", "urgent"],
            duplicate: "use-existing",
        });

        const media = yield* ResizeImage.enqueue({
            imageId: "image_1",
            width: 1200,
            height: 800,
        }, {
            tags: ["demo", "media"],
        });

        // Worker scenario:
        // `Jobs.run` is the long-running worker runtime. In this demo we
        // time it out after a moment so the script exits. A real worker
        // process would run it forever and provide service layers such as
        // EmailServiceLive or HttpClientLive alongside WorkerLive.
        // Calling `enqueue` makes a process a producer. Calling `Jobs.run`
        // makes it a worker. We do not need a public `role` switch for that.
        yield* Jobs.run.pipe(Effect.timeoutOption("250 millis"));

        // Operational visibility scenario:
        // Listing jobs and queue info is a tiny preview of the future
        // dashboard/read-model APIs. The dream version adds event timelines,
        // explainability, blocked reasons, and safe search projections.
        const records = yield* Job.list();
        const queueInfo = yield* Jobs.queues.info;

        console.log("handles", { email, webhook, media });
        console.log("queues", queueInfo);
        console.log(
            "jobs",
            records.map((record) => ({
                id: record.id,
                name: record.name,
                queue: record.queue,
                status: record.status,
                attempt: record.attempt,
                executions: record.executions,
                snoozes: record.snoozes,
            })),
        );

        const mediaRecord = yield* Job.find(media.id);

        if (Option.isSome(mediaRecord)) {
            // Runtime control scenario:
            // Admin actions such as cancel, retry, snooze, pause, and scale
            // should be first-class Effect operations.
            yield* Jobs.jobs.cancel(mediaRecord.value.id, {
                reason: "demo cleanup",
            });
        }
    })
    await Jobs.runPromise(
        program.pipe(Effect.provide(DemoLive)),
    );

    await Jobs.dispose();

    // Keep the production config example referenced so TypeScript does not treat
    // it as dead code when this file is used as a teaching artifact.
    void makeProductionRuntimeExample;
}
