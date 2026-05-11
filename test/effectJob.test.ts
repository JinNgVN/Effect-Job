import { Duration, Effect, Fiber, Option, Schema } from "effect";
import { describe, expect, it } from "vitest";

import { Job, JobCatalog, JobSystem, Queue, pruner, rescuer } from "../src";

describe("JobSystem", () => {
    it("wires runtime, inline handlers, and workers from one config object", async () => {
        const Catalog = JobCatalog.define({
            queues: {
                demo: Queue.define(),
            },
        });
        const ConfiguredJob = Catalog.job({
            name: "demo.configured",
            queue: "demo",
            payload: Schema.Struct({
                message: Schema.String,
            }),
            run: ({ payload }) =>
                Effect.logInfo(`configured handler: ${payload.message}`),
        });
        const Jobs = JobSystem.memory({
            catalog: Catalog,
            jobs: [ConfiguredJob],
            queues: {
                demo: { concurrency: 1, pollInterval: "10 millis" },
            },
        });

        const record = await Jobs.runPromise(
            Effect.gen(function* () {
                const handle = yield* ConfiguredJob.enqueue({ message: "hello" });
                yield* Jobs.worker().pipe(Effect.timeoutOption("50 millis"));
                return yield* Job.find(handle.id);
            }),
        );

        await Jobs.dispose();

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("completed");
            expect(record.value.attempt).toBe(1);
        }
    });

    it("supports inline run handlers as beginner sugar", async () => {
        const Catalog = JobCatalog.define();
        let handled = false;
        const InlineJob = Catalog.job({
            name: "demo.inline",
            payload: Schema.Struct({ message: Schema.String }),
            run: ({ payload }) =>
                Effect.sync(() => {
                    handled = payload.message === "hello";
                }),
        });
        const Jobs = JobSystem.memory({ catalog: Catalog, jobs: [InlineJob] });

        await Jobs.runPromise(
            Effect.gen(function* () {
                yield* InlineJob.enqueue({ message: "hello" });
                yield* Jobs.worker({ pollInterval: "10 millis" }).pipe(
                    Effect.timeoutOption("50 millis"),
                );
            }),
        );

        expect(handled).toBe(true);
    });

    it("runs plugin lifecycle hooks", async () => {
        const events: Array<string> = [];
        const push = (event: string) =>
            Effect.sync(() => {
                events.push(event);
            });
        const Catalog = JobCatalog.define({
            queues: {
                demo: Queue.define(),
            },
        });
        const PluginJob = Catalog.job({
            name: "demo.plugin",
            queue: "demo",
            payload: Schema.Struct({ message: Schema.String }),
            run: () => Effect.void,
        });
        const Jobs = JobSystem.memory({
            catalog: Catalog,
            jobs: [PluginJob],
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

        await Jobs.runPromise(
            Effect.gen(function* () {
                yield* PluginJob.enqueue({ message: "hello" });
                yield* Jobs.worker().pipe(Effect.timeoutOption("50 millis"));
            }),
        );

        expect(events).toEqual([
            "enqueued:demo.plugin:available",
            "worker:started",
            "started:demo.plugin:executing",
            "completed:demo.plugin:completed",
        ]);
    });

    it("wakes workers when a job is inserted", async () => {
        const Catalog = JobCatalog.define({
            queues: {
                demo: Queue.define(),
            },
        });
        const NotifyJob = Catalog.job({
            name: "demo.notifier",
            queue: "demo",
            payload: Schema.Struct({ message: Schema.String }),
            run: () => Effect.void,
        });
        const Jobs = JobSystem.memory({
            catalog: Catalog,
            jobs: [NotifyJob],
            queues: {
                demo: { concurrency: 1, pollInterval: "1 hour" },
            },
        });

        const record = await Jobs.runPromise(
            Effect.gen(function* () {
                const worker = yield* Jobs.worker().pipe(
                    Effect.forkChild({ startImmediately: true }),
                );

                yield* Effect.sleep("10 millis");

                const handle = yield* NotifyJob.enqueue({ message: "hello" });

                yield* Effect.sleep("30 millis");
                yield* Fiber.interrupt(worker);

                return yield* Job.find(handle.id);
            }),
        );

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("completed");
        }
    });

    it("reports catalog queues and configured worker queues", async () => {
        const Catalog = JobCatalog.define({
            queues: {
                mailers: Queue.define(),
                billing: Queue.define(),
            },
        });
        const Jobs = JobSystem.memory({
            catalog: Catalog,
            queues: {
                mailers: { concurrency: 2 },
            },
        });

        const queues = await Jobs.runPromise(Jobs.queues.info);

        expect(queues).toEqual({
            declared: ["billing", "default", "mailers"],
            workers: ["mailers"],
        });
    });

    it("exposes advanced surfaces as explicit shells", async () => {
        const Catalog = JobCatalog.define();
        const Jobs = JobSystem.memory({ catalog: Catalog });

        expect(Jobs.middleware.logging()).toEqual({ name: "logging" });
        expect(Jobs.extension({ name: "tenant-context" })).toEqual({
            name: "tenant-context",
        });

        await expect(Jobs.runPromise(Jobs.queues.pause("default"))).rejects.toMatchObject({
            _tag: "JobFeatureNotImplementedError",
            feature: "queues.pause",
        });
        await expect(Jobs.runPromise(Jobs.schedules.list)).rejects.toMatchObject({
            _tag: "JobFeatureNotImplementedError",
            feature: "schedules.list",
        });
    });

    it("can run bundled maintenance plugins", async () => {
        const Catalog = JobCatalog.define({
            queues: {
                demo: Queue.define(),
            },
        });
        const PruneJob = Catalog.job({
            name: "demo.prune",
            queue: "demo",
            payload: Schema.Struct({ message: Schema.String }),
            run: () => Effect.void,
        });
        const RescueJob = Catalog.job({
            name: "demo.rescue",
            queue: "demo",
            payload: Schema.Struct({ message: Schema.String }),
            run: () => Effect.never,
        });
        const Jobs = JobSystem.memory({
            catalog: Catalog,
            jobs: [PruneJob, RescueJob],
            queues: {
                demo: { concurrency: 1, pollInterval: "10 millis" },
            },
            shutdownGracePeriod: Duration.millis(0),
            plugins: [
                pruner({ every: "10 millis", olderThan: Duration.millis(0) }),
                rescuer({ every: "10 millis", rescueAfter: Duration.millis(0) }),
            ],
        });

        const result = await Jobs.runPromise(
            Effect.gen(function* () {
                const prune = yield* PruneJob.enqueue({ message: "prune" });
                const rescue = yield* RescueJob.enqueue({ message: "rescue" });
                yield* Jobs.worker().pipe(Effect.timeoutOption("90 millis"));
                const pruned = yield* Job.find(prune.id);
                const rescued = yield* Job.find(rescue.id);

                return { pruned, rescued };
            }),
        );

        expect(Option.isNone(result.pruned)).toBe(true);
        expect(Option.isSome(result.rescued)).toBe(true);

        if (Option.isSome(result.rescued)) {
            expect(result.rescued.value.status).toBe("available");
        }
    });
});
