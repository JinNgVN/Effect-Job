import { Deferred, Duration, Effect, Fiber, Option, Schema } from "effect";
import { describe, expect, it } from "vitest";

import { Job, JobCatalog, JobSystem, Queue } from "../src";

describe("Worker.run", () => {
    it("claims and completes one available job", async () => {
        const Catalog = JobCatalog.define();
        const WorkJob = Catalog.job({
            name: "demo.work",
            payload: Schema.Struct({
                message: Schema.String,
            }),
            run: ({ payload }) =>
                Effect.sync(() => {
                    handled = payload.message;
                }),
        });
        let handled: string | undefined;
        const Jobs = JobSystem.memory({
            catalog: Catalog,
            jobs: [WorkJob],
        });

        const record = await Jobs.runPromise(
            Effect.gen(function* () {
                const handle = yield* WorkJob.enqueue({ message: "hello" });
                yield* Jobs.worker({ pollInterval: "10 millis" }).pipe(
                    Effect.timeoutOption("50 millis"),
                );
                return yield* Job.find(handle.id);
            }),
        );

        expect(handled).toBe("hello");
        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("completed");
            expect(record.value.attempt).toBe(1);
        }
    });

    it("only claims jobs from configured queues", async () => {
        const Catalog = JobCatalog.define({
            queues: {
                mailers: Queue.define(),
                billing: Queue.define(),
            },
        });
        const MailJob = Catalog.job({
            name: "demo.mail",
            queue: "mailers",
            payload: Schema.Struct({ id: Schema.String }),
            run: () => Effect.void,
        });
        const BillingJob = Catalog.job({
            name: "demo.billing",
            queue: "billing",
            payload: Schema.Struct({ id: Schema.String }),
            run: () => Effect.void,
        });
        const Jobs = JobSystem.memory({
            catalog: Catalog,
            jobs: [MailJob, BillingJob],
            queues: {
                billing: { concurrency: 1, pollInterval: "10 millis" },
            },
        });

        const records = await Jobs.runPromise(
            Effect.gen(function* () {
                yield* MailJob.enqueue({ id: "mail" });
                yield* BillingJob.enqueue({ id: "billing" });
                yield* Jobs.worker().pipe(Effect.timeoutOption("50 millis"));
                return yield* Job.list();
            }),
        );

        expect(records.find((record) => record.name === "demo.mail")?.status).toBe(
            "available",
        );
        expect(
            records.find((record) => record.name === "demo.billing")?.status,
        ).toBe("completed");
    });

    it("retries and then discards failed jobs after attempts are exhausted", async () => {
        const Catalog = JobCatalog.define();
        const RetryJob = Catalog.job({
            name: "demo.retry",
            payload: Schema.Struct({ message: Schema.String }),
            attempts: {
                max: 2,
                backoff: () => Duration.millis(0),
            },
            run: () => Effect.fail("boom"),
        });
        const Jobs = JobSystem.memory({
            catalog: Catalog,
            jobs: [RetryJob],
        });

        const record = await Jobs.runPromise(
            Effect.gen(function* () {
                const handle = yield* RetryJob.enqueue({ message: "hello" });
                yield* Jobs.worker({ pollInterval: "10 millis" }).pipe(
                    Effect.timeoutOption("250 millis"),
                );
                return yield* Job.find(handle.id);
            }),
        );

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("discarded");
            expect(record.value.attempt).toBe(2);
            expect(record.value.errors).toHaveLength(2);
        }
    });

    it("supports explicit cancel, discard, and snooze outcomes from Jobs helpers", async () => {
        const Catalog = JobCatalog.define({
            queues: {
                outcomes: Queue.define(),
            },
        });
        let Jobs: ReturnType<typeof JobSystem.memory>;
        const CancelJob = Catalog.job({
            name: "demo.cancel",
            queue: "outcomes",
            payload: Schema.Struct({ id: Schema.String }),
            run: () => Jobs.cancel("not needed"),
        });
        const DiscardJob = Catalog.job({
            name: "demo.discard",
            queue: "outcomes",
            payload: Schema.Struct({ id: Schema.String }),
            run: () => Jobs.discard("permanent"),
        });
        const SnoozeJob = Catalog.job({
            name: "demo.snooze",
            queue: "outcomes",
            payload: Schema.Struct({ id: Schema.String }),
            run: () => Jobs.snooze("30 seconds", "wait"),
        });
        Jobs = JobSystem.memory({
            catalog: Catalog,
            jobs: [CancelJob, DiscardJob, SnoozeJob],
            queues: {
                outcomes: { concurrency: 1, pollInterval: "10 millis" },
            },
        });

        const records = await Jobs.runPromise(
            Effect.gen(function* () {
                yield* CancelJob.enqueue({ id: "cancel" });
                yield* DiscardJob.enqueue({ id: "discard" });
                yield* SnoozeJob.enqueue({ id: "snooze" });
                yield* Jobs.worker().pipe(Effect.timeoutOption("120 millis"));
                return yield* Job.list();
            }),
        );

        expect(records.find((record) => record.name === "demo.cancel")?.status).toBe(
            "cancelled",
        );
        expect(
            records.find((record) => record.name === "demo.discard")?.status,
        ).toBe("discarded");
        expect(records.find((record) => record.name === "demo.snooze")?.status).toBe(
            "scheduled",
        );
    });

    it("treats handler timeouts as job failures", async () => {
        const Catalog = JobCatalog.define();
        const TimeoutJob = Catalog.job({
            name: "demo.timeout",
            payload: Schema.Struct({ message: Schema.String }),
            attempts: 1,
            timeout: "10 millis",
            run: () => Effect.sleep("1 second"),
        });
        const Jobs = JobSystem.memory({
            catalog: Catalog,
            jobs: [TimeoutJob],
        });
        const record = await Jobs.runPromise(
            Effect.gen(function* () {
                const handle = yield* TimeoutJob.enqueue({ message: "hello" });
                yield* Jobs.worker({ pollInterval: "10 millis" }).pipe(
                    Effect.timeoutOption("250 millis"),
                );
                return yield* Job.find(handle.id);
            }),
        );

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("discarded");
            expect(record.value.errors).toHaveLength(1);
        }
    });

    it("passes job context to handlers", async () => {
        const Catalog = JobCatalog.define({
            queues: {
                context: Queue.define(),
            },
        });
        const ContextJob = Catalog.job({
            name: "demo.context",
            queue: "context",
            payload: Schema.Struct({ message: Schema.String }),
            run: ({ job }) =>
                Effect.sync(() => {
                    seen = {
                        name: job.name,
                        queue: job.queue,
                        attempt: job.attempt,
                        meta: job.meta,
                        tags: job.tags,
                        attemptedBy: job.attemptedBy,
                    };
                }),
        });
        let seen:
            | {
                  readonly name: string;
                  readonly queue: string;
                  readonly attempt: number;
                  readonly meta: Record<string, unknown>;
                  readonly tags: ReadonlyArray<string>;
                  readonly attemptedBy: ReadonlyArray<string>;
              }
            | undefined;
        const Jobs = JobSystem.memory({
            catalog: Catalog,
            jobs: [ContextJob],
            queues: {
                context: { concurrency: 1, pollInterval: "10 millis" },
            },
        });

        await Jobs.runPromise(
            Effect.gen(function* () {
                yield* ContextJob.enqueue(
                    { message: "hello" },
                    { meta: { tenantId: "tenant_1" }, tags: ["context"] },
                );
                yield* Jobs.worker({ workerId: "worker-test" }).pipe(
                    Effect.timeoutOption("100 millis"),
                );
            }),
        );

        expect(seen).toEqual({
            name: "demo.context",
            queue: "context",
            attempt: 1,
            meta: { tenantId: "tenant_1" },
            tags: ["context"],
            attemptedBy: ["worker-test"],
        });
    });

    it("waits for active jobs during shutdown grace period", async () => {
        const Catalog = JobCatalog.define({
            queues: {
                shutdown: Queue.define(),
            },
        });
        const ShutdownJob = Catalog.job({
            name: "demo.shutdown-grace",
            queue: "shutdown",
            payload: Schema.Struct({ message: Schema.String }),
            run: () =>
                Effect.gen(function* () {
                    yield* Deferred.succeed(started, undefined);
                    yield* Effect.sleep("30 millis");
                }),
        });
        const started = Deferred.makeUnsafe<void>();
        const Jobs = JobSystem.memory({
            catalog: Catalog,
            jobs: [ShutdownJob],
            queues: {
                shutdown: { concurrency: 1, pollInterval: "10 millis" },
            },
            shutdownGracePeriod: "100 millis",
        });

        const record = await Jobs.runPromise(
            Effect.gen(function* () {
                const handle = yield* ShutdownJob.enqueue({ message: "hello" });
                const worker = yield* Jobs.worker().pipe(Effect.forkChild);

                yield* Deferred.await(started);
                yield* Fiber.interrupt(worker);

                return yield* Job.find(handle.id);
            }),
        );

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("completed");
        }
    });
});
