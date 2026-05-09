import { Deferred, Duration, Effect, Fiber, Layer, Option, Schema } from "effect";
import { describe, expect, it } from "vitest";

import { Job, JobEngine, JobEngineMemory, JobRegistryMemory, Worker } from "../src";

describe("Worker.run", () => {
    it("claims and completes one available job", async () => {
        const job = Job.make({
            name: "demo.work",
            payload: Schema.Struct({
                message: Schema.String,
            }),
        });
        let handled: string | undefined;
        const JobLive = job.toLayer((payload) =>
            Effect.sync(() => {
                handled = payload.message;
            }),
        );
        const AppLive = Layer.mergeAll(
            JobLive.pipe(Layer.provide(JobRegistryMemory)),
            JobEngineMemory,
        );
        const program = Effect.gen(function* () {
            const handle = yield* job.new({ message: "hello" }).pipe(Job.insert);
            yield* Worker.run({ pollInterval: "10 millis" }).pipe(
                Effect.timeoutOption("50 millis"),
            );
            const engine = yield* JobEngine;
            const record = yield* engine.find(handle.id);

            return { record };
        }).pipe(Effect.provide(AppLive));

        const result = await Effect.runPromise(program);

        expect(handled).toBe("hello");
        expect(Option.isSome(result.record)).toBe(true);

        if (Option.isSome(result.record)) {
            expect(result.record.value.status).toBe("completed");
            expect(result.record.value.attempt).toBe(1);
        }
    });

    it("keeps running when no job is available", async () => {
        const program = Worker.run({ pollInterval: "10 millis" }).pipe(
            Effect.timeoutOption("50 millis"),
            Effect.provide(
                Layer.mergeAll(JobRegistryMemory, JobEngineMemory),
            ),
        );

        const result = await Effect.runPromise(program);

        expect(Option.isNone(result)).toBe(true);
    });

    it("only claims jobs from the requested queue", async () => {
        const mailJob = Job.make({
            name: "demo.mail",
            queue: "mailers",
            payload: Schema.Struct({
                id: Schema.String,
            }),
        });
        const billingJob = Job.make({
            name: "demo.billing",
            queue: "billing",
            payload: Schema.Struct({
                id: Schema.String,
            }),
        });
        const JobsLive = Layer.mergeAll(
            mailJob.toLayer(() => Effect.void),
            billingJob.toLayer(() => Effect.void),
        ).pipe(Layer.provide(JobRegistryMemory));
        const AppLive = Layer.mergeAll(JobsLive, JobEngineMemory);
        const program = Effect.gen(function* () {
            yield* mailJob.new({ id: "mail" }).pipe(Job.insert);
            yield* billingJob.new({ id: "billing" }).pipe(Job.insert);
            yield* Worker.run({
                queues: {
                    billing: { concurrency: 1, pollInterval: "10 millis" },
                },
            }).pipe(Effect.timeoutOption("50 millis"));
            const engine = yield* JobEngine;
            const records = yield* engine.list();

            return { records };
        }).pipe(Effect.provide(AppLive));

        const result = await Effect.runPromise(program);

        expect(
            result.records.find((record) => record.name === "demo.mail")
                ?.status,
        ).toBe("available");
        expect(
            result.records.find((record) => record.name === "demo.billing")
                ?.status,
        ).toBe("completed");
    });

    it("claims lower priority numbers first like Oban", async () => {
        const handled: Array<string> = [];
        const job = Job.make({
            name: "demo.priority",
            queue: "priority",
            payload: Schema.Struct({
                id: Schema.String,
            }),
        });
        const JobLive = job.toLayer((payload) =>
            Effect.sync(() => {
                handled.push(payload.id);
            }),
        );
        const AppLive = Layer.mergeAll(
            JobLive.pipe(Layer.provide(JobRegistryMemory)),
            JobEngineMemory,
        );
        const program = Effect.gen(function* () {
            yield* job.new({ id: "low" }, { priority: 9 }).pipe(Job.insert);
            yield* job.new({ id: "high" }, { priority: 0 }).pipe(Job.insert);
            yield* Worker.run({
                queues: {
                    priority: { concurrency: 1, pollInterval: "10 millis" },
                },
            }).pipe(Effect.timeoutOption("60 millis"));
        }).pipe(Effect.provide(AppLive));

        await Effect.runPromise(program);

        expect(handled).toEqual(["high", "low"]);
    });

    it("discards failed jobs after attempts are exhausted", async () => {
        const job = Job.make({
            name: "demo.retry",
            payload: Schema.Struct({
                message: Schema.String,
            }),
            error: Schema.String,
            attempts: 2,
            backoff: () => Duration.millis(0),
        });
        const JobLive = job.toLayer(() => Effect.fail("boom"));
        const AppLive = Layer.mergeAll(
            JobLive.pipe(Layer.provide(JobRegistryMemory)),
            JobEngineMemory,
        );
        const program = Effect.gen(function* () {
            const handle = yield* job.new({ message: "hello" }).pipe(Job.insert);
            yield* Worker.run({ pollInterval: "10 millis" }).pipe(
                Effect.timeoutOption("50 millis"),
            );
            const engine = yield* JobEngine;
            return yield* engine.find(handle.id);
        }).pipe(Effect.provide(AppLive));

        const record = await Effect.runPromise(program);

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("discarded");
            expect(record.value.attempt).toBe(2);
            expect(record.value.discardedAt).toBeInstanceOf(Date);
            expect(record.value.errors).toHaveLength(2);
        }
    });

    it("schedules failed jobs with backoff while attempts remain", async () => {
        const job = Job.make({
            name: "demo.backoff",
            payload: Schema.Struct({
                message: Schema.String,
            }),
            error: Schema.String,
            attempts: 2,
            backoff: () => Duration.seconds(30),
        });
        const JobLive = job.toLayer(() => Effect.fail("boom"));
        const AppLive = Layer.mergeAll(
            JobLive.pipe(Layer.provide(JobRegistryMemory)),
            JobEngineMemory,
        );
        const program = Effect.gen(function* () {
            const before = new Date();
            const handle = yield* job.new({ message: "hello" }).pipe(Job.insert);
            yield* Worker.run({ pollInterval: "10 millis" }).pipe(
                Effect.timeoutOption("50 millis"),
            );
            const engine = yield* JobEngine;
            const record = yield* engine.find(handle.id);

            return { before, record };
        }).pipe(Effect.provide(AppLive));

        const result = await Effect.runPromise(program);

        expect(Option.isSome(result.record)).toBe(true);

        if (Option.isSome(result.record)) {
            expect(result.record.value.status).toBe("retryable");
            expect(result.record.value.attempt).toBe(1);
            expect(result.record.value.runAt.getTime()).toBeGreaterThan(
                result.before.getTime(),
            );
        }
    });

    it("treats handler timeouts as job failures", async () => {
        const job = Job.make({
            name: "demo.timeout",
            payload: Schema.Struct({
                message: Schema.String,
            }),
            attempts: 1,
            timeout: "10 millis",
        });
        const JobLive = job.toLayer(() => Effect.sleep("1 second"));
        const AppLive = Layer.mergeAll(
            JobLive.pipe(Layer.provide(JobRegistryMemory)),
            JobEngineMemory,
        );
        const program = Effect.gen(function* () {
            const handle = yield* job.new({ message: "hello" }).pipe(Job.insert);
            yield* Worker.run({ pollInterval: "10 millis" }).pipe(
                Effect.timeoutOption("100 millis"),
            );
            const engine = yield* JobEngine;
            return yield* engine.find(handle.id);
        }).pipe(Effect.provide(AppLive));

        const record = await Effect.runPromise(program);

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("discarded");
            expect(record.value.attempt).toBe(1);
            expect(record.value.discardedAt).toBeInstanceOf(Date);
            expect(record.value.errors).toHaveLength(1);
        }
    });

    it("cancels jobs without retrying when handlers use Job.cancel", async () => {
        const job = Job.make({
            name: "demo.cancel",
            payload: Schema.Struct({
                message: Schema.String,
            }),
            error: Schema.Unknown,
            attempts: 3,
        });
        const JobLive = job.toLayer(() => Job.cancel("not needed"));
        const AppLive = Layer.mergeAll(
            JobLive.pipe(Layer.provide(JobRegistryMemory)),
            JobEngineMemory,
        );
        const program = Effect.gen(function* () {
            const handle = yield* job.new({ message: "hello" }).pipe(Job.insert);
            yield* Worker.run({ pollInterval: "10 millis" }).pipe(
                Effect.timeoutOption("100 millis"),
            );
            const engine = yield* JobEngine;
            return yield* engine.find(handle.id);
        }).pipe(Effect.provide(AppLive));

        const record = await Effect.runPromise(program);

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("cancelled");
            expect(record.value.attempt).toBe(1);
            expect(record.value.cancelledAt).toBeInstanceOf(Date);
            expect(record.value.errors).toHaveLength(1);
            expect(record.value.errors[0]?.message).toBe("not needed");
            expect(record.value.errors[0]?.kind).toBe("unknown");
        }
    });

    it("snoozes jobs for later without consuming retry budget", async () => {
        const job = Job.make({
            name: "demo.snooze",
            payload: Schema.Struct({
                message: Schema.String,
            }),
            error: Schema.Unknown,
            attempts: 3,
        });
        const JobLive = job.toLayer(() => Job.snooze("30 seconds"));
        const AppLive = Layer.mergeAll(
            JobLive.pipe(Layer.provide(JobRegistryMemory)),
            JobEngineMemory,
        );
        const program = Effect.gen(function* () {
            const before = new Date();
            const handle = yield* job.new({ message: "hello" }).pipe(Job.insert);
            yield* Worker.run({ pollInterval: "10 millis" }).pipe(
                Effect.timeoutOption("100 millis"),
            );
            const engine = yield* JobEngine;
            const record = yield* engine.find(handle.id);

            return { before, record };
        }).pipe(Effect.provide(AppLive));

        const result = await Effect.runPromise(program);

        expect(Option.isSome(result.record)).toBe(true);

        if (Option.isSome(result.record)) {
            expect(result.record.value.status).toBe("scheduled");
            expect(result.record.value.attempt).toBe(1);
            expect(result.record.value.maxAttempts).toBe(4);
            expect(result.record.value.errors).toHaveLength(0);
            expect(result.record.value.runAt.getTime()).toBeGreaterThan(
                result.before.getTime(),
            );
        }
    });

    it("passes job context to handlers", async () => {
        const job = Job.make({
            name: "demo.context",
            queue: "context",
            payload: Schema.Struct({
                message: Schema.String,
            }),
        });
        let seen:
            | {
                  readonly name: string;
                  readonly queue: string;
                  readonly attempt: number;
                  readonly maxAttempts: number;
                  readonly meta: Record<string, unknown>;
                  readonly tags: ReadonlyArray<string>;
                  readonly attemptedBy: ReadonlyArray<string>;
              }
            | undefined;
        const JobLive = job.toLayer((_, context) =>
            Effect.sync(() => {
                seen = {
                    name: context.name,
                    queue: context.queue,
                    attempt: context.attempt,
                    maxAttempts: context.maxAttempts,
                    meta: context.meta,
                    tags: context.tags,
                    attemptedBy: context.attemptedBy,
                };
            }),
        );
        const AppLive = Layer.mergeAll(
            JobLive.pipe(Layer.provide(JobRegistryMemory)),
            JobEngineMemory,
        );
        const program = Effect.gen(function* () {
            yield* job.new({ message: "hello" }, { meta: { tenantId: "tenant_1" }, tags: ["context"] }).pipe(Job.insert);
            yield* Worker.run({
                queues: { context: { concurrency: 1, pollInterval: "10 millis" } },
                workerId: "worker-test",
            }).pipe(Effect.timeoutOption("100 millis"));
        }).pipe(Effect.provide(AppLive));

        await Effect.runPromise(program);

        expect(seen).toEqual({
            name: "demo.context",
            queue: "context",
            attempt: 1,
            maxAttempts: 20,
            meta: { tenantId: "tenant_1" },
            tags: ["context"],
            attemptedBy: ["worker-test"],
        });
    });

    it("waits for active jobs during shutdown grace period", async () => {
        const job = Job.make({
            name: "demo.shutdown-grace",
            queue: "shutdown",
            payload: Schema.Struct({
                message: Schema.String,
            }),
        });
        const started = Deferred.makeUnsafe<void>();
        const JobLive = job.toLayer(() =>
            Effect.gen(function* () {
                yield* Deferred.succeed(started, undefined);
                yield* Effect.sleep("30 millis");
            }),
        );
        const AppLive = Layer.mergeAll(
            JobLive.pipe(Layer.provide(JobRegistryMemory)),
            JobEngineMemory,
        );
        const program = Effect.gen(function* () {
            const handle = yield* job.new({ message: "hello" }).pipe(Job.insert);
            const worker = yield* Worker.run({
                queues: {
                    shutdown: { concurrency: 1, pollInterval: "10 millis" },
                },
                shutdownGracePeriod: "100 millis",
            }).pipe(Effect.forkChild);

            yield* Deferred.await(started);
            yield* Fiber.interrupt(worker);

            const engine = yield* JobEngine;
            return yield* engine.find(handle.id);
        }).pipe(Effect.provide(AppLive));

        const record = await Effect.runPromise(program);

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("completed");
        }
    });
});
