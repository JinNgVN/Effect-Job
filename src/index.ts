// Public package entrypoint.
//
// When imported as a library these exports are all that run.
// When executed directly (`bun run src/index.ts`) the demo below
// walks through every major concept in the vNext API.

export * from "./catalog";
export * from "./databases";
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
// Live API walk-through (only runs when this file is the Bun entry point)
// ---------------------------------------------------------------------------

const isMain = typeof Bun !== "undefined" && import.meta.path === Bun.main;

if (isMain) {
    const { Effect, Schema } = await import("effect");

    // Import from internal modules so we don't self-reference.
    const { Job } = await import("./job");
    const { JobCatalog } = await import("./catalog");
    const { JobCommand } = await import("./job");
    const { JobSystem } = await import("./system");
    const { Queue } = await import("./queue");

    // ─── 1. DEFINE A CATALOG ────────────────────────────────────────────
    // A catalog declares which *named queues* your application uses.
    // Every catalog gets an implicit "default" queue; you can override it
    // or add more.
    // ─────────────────────────────────────────────────────────────────────

    const Catalog = JobCatalog.define({
        queues: {
            // "default" is always present; here we give it custom options.
            default: Queue.define({ concurrency: 5 }),

            // Additional queues are only created when you list them.
            mailers: Queue.define(),
            webhooks: Queue.define({ concurrency: 10 }),
            media: Queue.define({ concurrency: 2 }),
        },
    });

    // ─── 2. DEFINE JOBS ─────────────────────────────────────────────────
    // Jobs are created *from* a catalog so the compiler knows which queues
    // are valid.  A queue can be a static string, a dynamic function, or
    // even an Effect that picks based on the payload.
    // ─────────────────────────────────────────────────────────────────────

    // A simple job on the "default" queue (no explicit queue = "default").
    const Greet = Catalog.job({
        name: "greet",
        payload: Schema.Struct({
            name: Schema.String,
        }),
        run: ({ payload }) => Effect.logInfo(`Hello, ${payload.name}!`),
    });

    // A job with a custom queue, retry backoff, and a unique constraint.
    // `unique.key` is a *policy* — a value, sync fn, or Effect — that
    // produces a deduplication key.  Two jobs with the same
    // (name, queue, key) that are still active will be collapsed.
    const SendEmail = Catalog.job({
        name: "email.send",
        queue: "mailers", // type-safe: "mailer" would fail at compile time
        payload: Schema.Struct({
            userId: Schema.String,
            email: Schema.String,
            subject: Schema.String,
        }),
        // Maximum retry attempts (defaults to 20).
        attempts: {
            max: 5,
            // Oban-compatible backoff function.
            backoff: ({ attempt }) =>
                Math.trunc(Math.pow(attempt, 4) + 15) * 1000,
        },
        // Per-job timeout after which the worker stops waiting.
        timeout: "30 seconds",
        // Unique constraint: skip inserting if an active job with the
        // same key already exists.
        unique: {
            key: ({ payload }) => ["email", payload.userId, payload.subject],
            // Only de-duplicate while these statuses are active.
            while: ["available", "scheduled", "executing", "retryable"],
        },
        // The `run` handler is what the worker executes.
        // This is inline sugar; you can also wire it via `.toLayer()`.
        run: ({ payload }) =>
            Effect.logInfo(
                `Sending email to ${payload.email}: "${payload.subject}"`,
            ),
    });

    // A job that resolves its queue at enqueue time based on the payload.
    const DeliverWebhook = Catalog.job({
        name: "webhook.deliver",
        // Dynamic queue policy: the function receives the payload and
        // returns the queue name as a plain value or an Effect.
        queue: ({ payload }) =>
            payload.urgent
                ? "webhooks"
                : Queue.dynamic(`tenant:${payload.tenantId}`), // escape hatch
        payload: Schema.Struct({
            tenantId: Schema.String,
            endpointId: Schema.String,
            eventId: Schema.String,
            urgent: Schema.Boolean,
        }),
        run: ({ payload }) =>
            Effect.logInfo(
                `Delivering webhook ${payload.eventId} to tenant ${payload.tenantId}`,
            ),
    });

    // A job that shows explicit outcome helpers (cancel, discard, snooze)
    // inside its handler.  Because the handler references `Jobs` (defined
    // below), we use `let Jobs` so the closure captures the variable at
    // runtime, not at definition time.
    const MediaProcess = Catalog.job({
        name: "media.process",
        queue: "media",
        payload: Schema.Struct({
            mediaId: Schema.String,
        }),
        attempts: 3,
        timeout: "2 minutes",
        run: ({ payload }) =>
            Effect.gen(function* () {
                yield* Effect.logInfo(`Processing media ${payload.mediaId}...`);
                // Explicitly snooze the job for 10 seconds.
                // This does NOT consume a retry attempt.
                return yield* Jobs.snooze("10 seconds", "File not ready");
            }),
    });

    // ─── 3. ASSEMBLE THE SYSTEM ─────────────────────────────────────────
    // `JobSystem.memory(...)` creates a fully wired in-process runtime.
    // `JobSystem.postgres({ url: "..." })` for production Postgres.
    // `JobSystem.custom({ database: myLayer })` when you bring your own.
    // ─────────────────────────────────────────────────────────────────────

    // `let` declares `Jobs` first so MediaProcess's `run` closure can
    // see it.  The closure will read the value at runtime (after the
    // assignment below), so this is safe.
    let Jobs: ReturnType<typeof JobSystem.memory>;
    Jobs = JobSystem.memory({
        catalog: Catalog,
        // Inline `run` handlers on each job definition are registered
        // automatically.  No separate `handlers` config needed.
        jobs: [Greet, SendEmail, DeliverWebhook, MediaProcess],
        // `layers` is for service dependencies (DB clients, API stubs, etc).
        // For advanced cases where a handler lives outside the catalog,
        // use `JobSystem.custom` or compose `SomedJob.toLayer(run)` with
        // `Jobs.toLayer()` via `Layer.provide` after creation.
        // Worker queue configuration.
        queues: {
            default: { concurrency: 2, pollInterval: "50 millis" },
            mailers: { concurrency: 5, pollInterval: "100 millis" },
            webhooks: { concurrency: 3, pollInterval: "100 millis" },
            media: { concurrency: 1, pollInterval: "200 millis" },
        },
        // Optional global defaults for all jobs.
        defaults: {
            attempts: 10,
            timeout: "60 seconds",
        },
        // Plugins that run in the worker scope.
        plugins: [
            {
                name: "demo-plugin",
                onWorkerStarted: () =>
                    Effect.logInfo("Worker started — plugins are alive"),
                onJobEnqueued: ({ job }) =>
                    Effect.logInfo(
                        `Job enqueued: ${job.name} (${job.id.slice(0, 8)}...)`,
                    ),
                onJobStarted: ({ job }) =>
                    Effect.logInfo(
                        `Job started: ${job.name}, attempt ${job.attempt}`,
                    ),
                onJobCompleted: ({ job }) =>
                    Effect.logInfo(`Job completed: ${job.name}`),
            },
        ],
    });

    // ─── 4. ENQUEUE JOBS ────────────────────────────────────────────────
    // There are two paths:
    //   (a) Happy path:   job.enqueue(payload, options?)   — Effect
    //   (b) Explicit:     job.command(payload, options?)   — pure data
    //                     Jobs.insert(command)             — Effect
    //
    // `command` is pure so you can inspect, transform, compose, or even
    // discard it before persisting.  `enqueue` is sugar for command +
    // insert.
    // ─────────────────────────────────────────────────────────────────────

    // (a) Happy path — one-liner.
    const greetHandle = await Jobs.runPromise(Greet.enqueue({ name: "World" }));
    console.log(
        `  Enqueued greet: ${greetHandle.name} / ${greetHandle.id.slice(0, 8)}... (queue: ${greetHandle.queue})`,
    );

    // (b) Explicit command path with pipeable transformations.
    // eslint-disable-next-line prefer-const
    let emailHandle: { name: string; id: string; queue: string };
    emailHandle = await Jobs.runPromise(
        SendEmail
            // Build a pure command object.
            .command({
                userId: "user_1",
                email: "hello@example.com",
                subject: "Welcome!",
            })
            // Chain transformations — all pure, no side effects yet.
            .pipe(
                JobCommand.withMeta({ campaign: "onboarding" }),
                JobCommand.addTags(["welcome", "email"]),
                JobCommand.withPriority(3),
            )
            // Finally, insert — this is the Effect that persists.
            .pipe(Jobs.insert) as any,
    );
    console.log(
        `  Enqueued email: ${emailHandle.name} / ${emailHandle.id.slice(0, 8)}... (queue: ${emailHandle.queue})`,
    );

    // Enqueue a webhook — this one routes to "webhooks" because it's urgent.
    // Non-urgent webhooks would go to a dynamic tenant queue.
    const whHandle = await Jobs.runPromise(
        DeliverWebhook.enqueue({
            tenantId: "tenant_42",
            endpointId: "ep_1",
            eventId: "evt_1",
            urgent: true, // routes to "webhooks" (static queue in worker config)
        }),
    );
    console.log(
        `  Enqueued webhook: ${whHandle.name} / ${whHandle.id.slice(0, 8)}... (queue: ${whHandle.queue})`,
    );

    // Enqueue a media job (handler calls Jobs.snooze).
    const mediaHandle = await Jobs.runPromise(
        MediaProcess.enqueue({ mediaId: "img_99" }),
    );
    console.log(
        `  Enqueued media: ${mediaHandle.name} / ${mediaHandle.id.slice(0, 8)}... (queue: ${mediaHandle.queue})`,
    );

    // ─── 5. RUN WORKERS ─────────────────────────────────────────────────
    // `Jobs.worker(options?)` is an Effect that runs forever.
    // Here we fork it as a child fiber, let it run for a short window,
    // then interrupt to inspect results.
    // ─────────────────────────────────────────────────────────────────────

    console.log("\n  --- Starting workers ---\n");

    await Jobs.runPromise(
        // Start the worker and let it run for a short window.
        // `Effect.timeoutOption` automatically interrupts after the duration.
        Jobs.worker().pipe(Effect.timeoutOption("300 millis")),
    );

    // ─── 6. QUERY JOBS ──────────────────────────────────────────────────
    // `Job.list(options?)` and `Job.find(id)` inspect stored state.
    // ─────────────────────────────────────────────────────────────────────

    console.log("\n  --- Job query results ---\n");

    const allJobs = await Jobs.runPromise(Job.list());
    console.table(
        allJobs.map((j) => ({
            name: j.name,
            status: j.status,
            queue: j.queue,
            attempt: `${j.attempt}/${j.maxAttempts}`,
            priority: j.priority,
            idempotency: j.idempotencyKey ?? "—",
        })),
    );

    // ─── 7. JOB CONTROLS ────────────────────────────────────────────────
    // `Jobs.jobs.*` provides runtime controls: retry, cancel, snooze.
    // ─────────────────────────────────────────────────────────────────────

    // Cancel the greet job (if not already completed).
    if (greetHandle) {
        const before = await Jobs.runPromise(Job.find(greetHandle.id));

        if (before._tag === "Some" && before.value.status !== "completed") {
            await Jobs.runPromise(
                Jobs.jobs.cancel(greetHandle.id, {
                    reason: "Demo cleanup",
                }),
            );
            console.log(
                `\n  Cancelled greet job ${greetHandle.id.slice(0, 8)}...`,
            );
        }
    }

    // ─── 8. INSPECT ADVANCED SURFACES ───────────────────────────────────
    // These surfaces are typed API shells.  They currently return
    // `JobFeatureNotImplementedError` for features not yet built,
    // but they give you a feel for the intended API shape.
    // ─────────────────────────────────────────────────────────────────────

    console.log("\n  --- Advanced surface preview ---\n");

    // Queue information (implemented).
    const queueInfo = await Jobs.runPromise(Jobs.queues.info);
    console.log("  Queue info:", queueInfo);

    // Extension builder (passthrough).
    console.log("  Extension:", Jobs.extension({ name: "tenant-context" }));

    // Middleware builder (shell).
    console.log("  Middleware:", Jobs.middleware.logging());

    // Pause a queue (shell — not implemented yet).
    await Jobs.runPromise(Jobs.queues.pause("webhooks")).then(
        () => console.log("  Queue pause: ok"),
        (err: { _tag: string; feature: string }) =>
            console.log(`  Queue pause: ${err._tag} — ${err.feature}`),
    );

    // ─── 9. CLEANUP ─────────────────────────────────────────────────────
    // `dispose()` releases the ManagedRuntime and any scoped resources.
    // ─────────────────────────────────────────────────────────────────────

    await Jobs.dispose();
    console.log("\n  Done. ManagedRuntime disposed.");
}
