import { Duration, Effect, Fiber, Option, Schema } from "effect";
import { describe, expect, it } from "vitest";

import { effectJob, Job, memory, pruner, rescuer } from "../src";

describe("effectJob", () => {
    it("wires engine, handlers, and workers from one config object", async () => {
        const job = Job.make({
            name: "demo.configured",
            queue: "demo",
            payload: Schema.Struct({
                message: Schema.String,
            }),
        });
        const app = effectJob({
            database: memory(),
            handlers: [
                job.toLayer((payload) =>
                    Effect.logInfo(`configured handler: ${payload.message}`),
                ),
            ],
            queues: {
                demo: { concurrency: 1, pollInterval: "10 millis" },
            },
        });

        const handle = await job.new({ message: "hello" }).pipe(app.insert);

        await app.runPromise(
            app.worker().pipe(Effect.timeoutOption("50 millis")),
        );

        const record = await app.find(handle.id);

        await app.dispose();

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("completed");
            expect(record.value.attempt).toBe(1);
        }
    });

    it("runs plugin lifecycle hooks", async () => {
        const events: Array<string> = [];
        const push = (event: string) =>
            Effect.sync(() => {
                events.push(event);
            });
        const job = Job.make({
            name: "demo.plugin",
            queue: "demo",
            payload: Schema.Struct({
                message: Schema.String,
            }),
        });
        const app = effectJob({
            database: memory(),
            handlers: [job.toLayer(() => Effect.void)],
            queues: {
                demo: { concurrency: 1, pollInterval: "10 millis" },
            },
            plugins: [
                {
                    name: "events",
                    onJobEnqueued: ({ job }) =>
                        push(`enqueued:${job.name}:${job.status}`),
                    onWorkerStarted: () => push("worker:started"),
                    onJobStarted: ({ job }) =>
                        push(`started:${job.name}:${job.status}`),
                    onJobCompleted: ({ job }) =>
                        push(`completed:${job.name}:${job.status}`),
                },
            ],
        });

        await job.new({ message: "hello" }).pipe(app.insert);
        await app.runPromise(
            app.worker().pipe(Effect.timeoutOption("50 millis")),
        );
        await app.dispose();

        expect(events).toEqual([
            "enqueued:demo.plugin:available",
            "worker:started",
            "started:demo.plugin:executing",
            "completed:demo.plugin:completed",
        ]);
    });

    it("wakes workers when a job is inserted", async () => {
        const job = Job.make({
            name: "demo.notifier",
            queue: "demo",
            payload: Schema.Struct({
                message: Schema.String,
            }),
        });
        const app = effectJob({
            database: memory(),
            handlers: [job.toLayer(() => Effect.void)],
            queues: {
                demo: { concurrency: 1, pollInterval: "1 hour" },
            },
        });

        const record = await app.runPromise(
            Effect.gen(function* () {
                const worker = yield* app.worker().pipe(
                    Effect.forkChild({ startImmediately: true }),
                );

                yield* Effect.sleep("10 millis");

                const handle = yield* job
                    .new({ message: "hello" })
                    .pipe(Job.insert);

                yield* Effect.sleep("30 millis");
                yield* Fiber.interrupt(worker);

                return yield* Job.find(handle.id);
            }),
        );

        await app.dispose();

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("completed");
        }
    });

    it("can run the bundled pruner in the worker scope", async () => {
        const job = Job.make({
            name: "demo.prune",
            queue: "demo",
            payload: Schema.Struct({
                message: Schema.String,
            }),
        });
        const app = effectJob({
            database: memory(),
            handlers: [job.toLayer(() => Effect.void)],
            queues: {
                demo: { concurrency: 1, pollInterval: "10 millis" },
            },
            plugins: [
                pruner({
                    every: "10 millis",
                    olderThan: Duration.millis(0),
                }),
            ],
        });

        const handle = await job.new({ message: "hello" }).pipe(app.insert);

        await app.runPromise(
            app.worker().pipe(Effect.timeoutOption("80 millis")),
        );

        const record = await app.find(handle.id);

        await app.dispose();

        expect(Option.isNone(record)).toBe(true);
    });

    it("can run the bundled rescuer in the worker scope", async () => {
        const job = Job.make({
            name: "demo.rescuer",
            queue: "demo",
            payload: Schema.Struct({
                message: Schema.String,
            }),
        });
        const app = effectJob({
            database: memory(),
            handlers: [job.toLayer(() => Effect.never)],
            queues: {
                demo: { concurrency: 1, pollInterval: "10 millis" },
            },
            shutdownGracePeriod: Duration.millis(0),
            plugins: [
                rescuer({
                    every: "10 millis",
                    rescueAfter: Duration.millis(0),
                }),
            ],
        });

        const handle = await job.new({ message: "hello" }).pipe(app.insert);

        await app.runPromise(
            app.worker().pipe(Effect.timeoutOption("80 millis")),
        );

        const record = await app.find(handle.id);

        await app.dispose();

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("available");
            expect(record.value.attempt).toBe(1);
        }
    });

    it("reports declared and configured worker queues without scanning jobs", async () => {
        const mailJob = Job.make({
            name: "demo.mail",
            queue: "mailers",
            payload: Schema.Struct({
                message: Schema.String,
            }),
        });
        const billingJob = Job.make({
            name: "demo.billing",
            queue: "billing",
            payload: Schema.Struct({
                message: Schema.String,
            }),
        });
        const app = effectJob({
            database: memory(),
            handlers: [
                mailJob.toLayer(() => Effect.void),
                billingJob.toLayer(() => Effect.void),
            ],
            queues: {
                mailers: { concurrency: 2 },
            },
        });

        const queues = await app.queues();

        await app.dispose();

        expect(queues).toEqual({
            declared: ["billing", "mailers"],
            workers: ["mailers"],
        });
    });
});
