import { Deferred, Duration, Effect, Fiber, Layer, Option, Schema } from "effect";
import { describe, expect, it } from "vitest";

import {
    effectJob,
    Job,
    JobNotifierMemory,
    JobRegistryMemory,
    JobRuntimeLive,
    memory,
} from "../src";

const runPromise = <A, E>(effect: Effect.Effect<A, E, any>) =>
    Effect.runPromise(effect as Effect.Effect<A, E, never>);

describe("worker runtime", () => {
    it("claims and completes one available job", async () => {
        const Jobs = effectJob({
            queues: {
                work: { concurrency: 1, pollInterval: "10 millis" },
            },
        });
        const WorkJob = Jobs.define({
            name: "demo.work",
            queue: "work",
            payload: Schema.Struct({
                message: Schema.String,
            }),
        });
        let handled: string | undefined;
        const WorkJobLive = WorkJob.toLayer(({ payload }) =>
            Effect.sync(() => {
                handled = payload.message;
            }),
        );
        const Live = Layer.mergeAll(memory(), JobNotifierMemory, WorkJobLive);

        const record = await runPromise(
            Effect.gen(function* () {
                const handle = yield* WorkJob.enqueue({ message: "hello" });
                yield* Jobs.run.pipe(Effect.timeoutOption("50 millis"));
                return yield* Job.find(handle.id);
            }).pipe(
                Effect.provide(Live),
                Effect.provide(JobRuntimeLive(Jobs)),
                Effect.provide(JobRegistryMemory),
            ),
        );

        expect(handled).toBe("hello");
        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("completed");
            expect(record.value.attempt).toBe(0);
            expect(record.value.executions).toBe(1);
            expect(record.value.snoozes).toBe(0);
        }
    });

    it("only claims jobs from configured queues", async () => {
        const Jobs = effectJob({
            queues: {
                mailers: {},
                billing: { concurrency: 1, pollInterval: "10 millis" },
            },
        });
        const MailJob = Jobs.define({
            name: "demo.mail",
            queue: "mailers",
            payload: Schema.Struct({ id: Schema.String }),
        });
        const BillingJob = Jobs.define({
            name: "demo.billing",
            queue: "billing",
            payload: Schema.Struct({ id: Schema.String }),
        });
        const BillingJobLive = BillingJob.toLayer(() => Effect.void);
        const Live = Layer.mergeAll(memory(), JobNotifierMemory, BillingJobLive);

        const records = await runPromise(
            Effect.gen(function* () {
                yield* MailJob.enqueue({ id: "mail" });
                yield* BillingJob.enqueue({ id: "billing" });
                yield* Jobs.worker({
                    queues: {
                        billing: { concurrency: 1, pollInterval: "10 millis" },
                    },
                }).pipe(Effect.timeoutOption("50 millis"));
                return yield* Job.list();
            }).pipe(
                Effect.provide(Live),
                Effect.provide(JobRuntimeLive(Jobs)),
                Effect.provide(JobRegistryMemory),
            ),
        );

        expect(records.find((record) => record.name === "demo.mail")?.status).toBe(
            "available",
        );
        expect(
            records.find((record) => record.name === "demo.billing")?.status,
        ).toBe("completed");
    });

    it("retries and then discards failed jobs after attempts are exhausted", async () => {
        const Jobs = effectJob({
            queues: {
                retry: { concurrency: 1, pollInterval: "10 millis" },
            },
        });
        const RetryJob = Jobs.define({
            name: "demo.retry",
            queue: "retry",
            payload: Schema.Struct({ message: Schema.String }),
            attempts: {
                max: 2,
                backoff: () => Duration.millis(0),
            },
        });
        const RetryJobLive = RetryJob.toLayer(() => Effect.fail("boom"));
        const Live = Layer.mergeAll(memory(), JobNotifierMemory, RetryJobLive);

        const record = await runPromise(
            Effect.gen(function* () {
                const handle = yield* RetryJob.enqueue({ message: "hello" });
                yield* Jobs.run.pipe(Effect.timeoutOption("250 millis"));
                return yield* Job.find(handle.id);
            }).pipe(
                Effect.provide(Live),
                Effect.provide(JobRuntimeLive(Jobs)),
                Effect.provide(JobRegistryMemory),
            ),
        );

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("discarded");
            expect(record.value.attempt).toBe(2);
            expect(record.value.executions).toBe(2);
            expect(record.value.errors).toHaveLength(2);
        }
    });

    it("supports explicit cancel, discard, and snooze outcomes from Jobs helpers", async () => {
        const Jobs = effectJob({
            queues: {
                outcomes: { concurrency: 1, pollInterval: "10 millis" },
            },
        });
        const CancelJob = Jobs.define({
            name: "demo.cancel",
            queue: "outcomes",
            payload: Schema.Struct({ id: Schema.String }),
        });
        const DiscardJob = Jobs.define({
            name: "demo.discard",
            queue: "outcomes",
            payload: Schema.Struct({ id: Schema.String }),
        });
        const SnoozeJob = Jobs.define({
            name: "demo.snooze",
            queue: "outcomes",
            payload: Schema.Struct({ id: Schema.String }),
        });
        const WorkerLive = Layer.mergeAll(
            CancelJob.toLayer(() => Jobs.cancel("not needed")),
            DiscardJob.toLayer(() => Jobs.discard("permanent")),
            SnoozeJob.toLayer(() => Jobs.snooze("30 seconds", "wait")),
        );
        const Live = Layer.mergeAll(memory(), JobNotifierMemory, WorkerLive);

        const records = await runPromise(
            Effect.gen(function* () {
                yield* CancelJob.enqueue({ id: "cancel" });
                yield* DiscardJob.enqueue({ id: "discard" });
                yield* SnoozeJob.enqueue({ id: "snooze" });
                yield* Jobs.run.pipe(Effect.timeoutOption("120 millis"));
                return yield* Job.list();
            }).pipe(
                Effect.provide(Live),
                Effect.provide(JobRuntimeLive(Jobs)),
                Effect.provide(JobRegistryMemory),
            ),
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
        expect(records.find((record) => record.name === "demo.snooze")?.attempt).toBe(
            0,
        );
        expect(records.find((record) => record.name === "demo.snooze")?.snoozes).toBe(
            1,
        );
    });

    it("waits for active jobs during shutdown grace period", async () => {
        const Jobs = effectJob({
            queues: {
                shutdown: { concurrency: 1, pollInterval: "10 millis" },
            },
            shutdownGracePeriod: "100 millis",
        });
        const ShutdownJob = Jobs.define({
            name: "demo.shutdown-grace",
            queue: "shutdown",
            payload: Schema.Struct({ message: Schema.String }),
        });
        const started = Deferred.makeUnsafe<void>();
        const ShutdownJobLive = ShutdownJob.toLayer(() =>
            Effect.gen(function* () {
                yield* Deferred.succeed(started, undefined);
                yield* Effect.sleep("30 millis");
            }),
        );
        const Live = Layer.mergeAll(memory(), JobNotifierMemory, ShutdownJobLive);

        const record = await runPromise(
            Effect.gen(function* () {
                const handle = yield* ShutdownJob.enqueue({ message: "hello" });
                const worker = yield* Jobs.run.pipe(
                    Effect.forkChild,
                );

                yield* Deferred.await(started);
                yield* Fiber.interrupt(worker);

                return yield* Job.find(handle.id);
            }).pipe(
                Effect.provide(Live),
                Effect.provide(JobRuntimeLive(Jobs)),
                Effect.provide(JobRegistryMemory),
            ),
        );

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("completed");
        }
    });
});
