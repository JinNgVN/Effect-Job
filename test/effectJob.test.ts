import { Effect, Fiber, Layer, Option, Schema } from "effect";
import { describe, expect, it } from "vitest";

import {
    effectJob,
    Job,
    JobNotifierMemory,
    JobPluginsLive,
    JobRegistryMemory,
    JobRuntimeLive,
    memory,
    pruner,
    rescuer,
} from "../src";

const runPromise = <A, E>(effect: Effect.Effect<A, E, any>) =>
    Effect.runPromise(effect as Effect.Effect<A, E, never>);

describe("effectJob runtime", () => {
    it("wires runtime, handler layers, and workers from one config object", async () => {
        const Jobs = effectJob({
            queues: {
                demo: { concurrency: 1, pollInterval: "10 millis" },
            },
        });
        const ConfiguredJob = Jobs.define({
            name: "demo.configured",
            queue: "demo",
            payload: Schema.Struct({
                message: Schema.String,
            }),
        });
        const ConfiguredJobLive = ConfiguredJob.toLayer(({ payload }) =>
            Effect.logInfo(`configured handler: ${payload.message}`),
        );
        const Live = Layer.mergeAll(memory(), JobNotifierMemory, ConfiguredJobLive);

        const record = await runPromise(
            Effect.gen(function* () {
                const handle = yield* ConfiguredJob.enqueue({ message: "hello" });
                yield* Jobs.run.pipe(Effect.timeoutOption("50 millis"));
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
            expect(record.value.attempt).toBe(0);
            expect(record.value.executions).toBe(1);
        }
    });

    it("runs plugin lifecycle hooks", async () => {
        const events: Array<string> = [];
        const push = (event: string) =>
            Effect.sync(() => {
                events.push(event);
            });
        const Jobs = effectJob({
            queues: {
                demo: { concurrency: 1, pollInterval: "10 millis" },
            },
        });
        const PluginJob = Jobs.define({
            name: "demo.plugin",
            queue: "demo",
            payload: Schema.Struct({ message: Schema.String }),
        });
        const PluginJobLive = PluginJob.toLayer(() => Effect.void);
        const PluginsLive = JobPluginsLive({
            name: "events",
            onJobEnqueued: ({ job }) =>
                push(`enqueued:${job.name}:${job.status}`),
            onWorkerStarted: () => push("worker:started"),
            onJobStarted: ({ job }) =>
                push(`started:${job.name}:${job.status}`),
            onJobCompleted: ({ job }) =>
                push(`completed:${job.name}:${job.status}`),
        });
        const Live = Layer.mergeAll(memory(), JobNotifierMemory, PluginJobLive, PluginsLive);

        await runPromise(
            Effect.gen(function* () {
                yield* PluginJob.enqueue({ message: "hello" });
                yield* Jobs.run.pipe(Effect.timeoutOption("50 millis"));
            }).pipe(
                Effect.provide(Live),
                Effect.provide(JobRuntimeLive(Jobs)),
                Effect.provide(JobRegistryMemory),
            ),
        );

        expect(events).toEqual([
            "enqueued:demo.plugin:available",
            "worker:started",
            "started:demo.plugin:executing",
            "completed:demo.plugin:completed",
        ]);
    });

    it("wakes workers when a job is inserted", async () => {
        const Jobs = effectJob({
            queues: {
                demo: { concurrency: 1, pollInterval: "1 hour" },
            },
        });
        const NotifyJob = Jobs.define({
            name: "demo.notifier",
            queue: "demo",
            payload: Schema.Struct({ message: Schema.String }),
        });
        const NotifyJobLive = NotifyJob.toLayer(() => Effect.void);
        const Live = Layer.mergeAll(memory(), JobNotifierMemory, NotifyJobLive);

        const record = await runPromise(
            Effect.gen(function* () {
                const worker = yield* Jobs.run.pipe(
                    Effect.forkChild({ startImmediately: true }),
                );

                yield* Effect.sleep("10 millis");

                const handle = yield* NotifyJob.enqueue({ message: "hello" });

                yield* Effect.sleep("30 millis");
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

    it("reports configured worker queues", async () => {
        const Jobs = effectJob({
            queues: {
                mailers: { concurrency: 2 },
                billing: {},
            },
        });

        const queues = await runPromise(
            Jobs.queues.info.pipe(
                Effect.provide(JobRuntimeLive(Jobs)),
                Effect.provide(JobRegistryMemory),
            ),
        );

        expect(queues).toEqual({
            declared: ["billing", "mailers"],
            workers: ["billing", "mailers"],
        });
    });

    it("exposes advanced surfaces as explicit shells", async () => {
        const Jobs = effectJob({
            queues: {
                default: {},
            },
        });

        expect(Jobs.middleware.logging()).toEqual({ name: "logging" });
        expect(Jobs.extension({ name: "tenant-context" })).toEqual({
            name: "tenant-context",
        });

        await expect(
            runPromise(
                Jobs.queues.pause("default").pipe(
                    Effect.provide(JobRuntimeLive(Jobs)),
                    Effect.provide(JobRegistryMemory),
                ),
            ),
        ).rejects.toMatchObject({
            _tag: "JobFeatureNotImplementedError",
            feature: "queues.pause",
        });
        await expect(
            runPromise(
                Jobs.schedules.list.pipe(
                    Effect.provide(JobRuntimeLive(Jobs)),
                    Effect.provide(JobRegistryMemory),
                ),
            ),
        ).rejects.toMatchObject({
            _tag: "JobFeatureNotImplementedError",
            feature: "schedules.list",
        });
    });

    it("can run bundled maintenance plugins", async () => {
        const Jobs = effectJob({
            queues: {
                demo: { concurrency: 1, pollInterval: "10 millis" },
            },
            shutdownGracePeriod: "0 millis",
        });
        const PruneJob = Jobs.define({
            name: "demo.prune",
            queue: "demo",
            payload: Schema.Struct({ message: Schema.String }),
        });
        const RescueJob = Jobs.define({
            name: "demo.rescue",
            queue: "demo",
            payload: Schema.Struct({ message: Schema.String }),
        });
        const WorkerLive = Layer.mergeAll(
            PruneJob.toLayer(() => Effect.void),
            RescueJob.toLayer(() => Effect.never),
        );
        const Live = Layer.mergeAll(
            memory(),
            JobNotifierMemory,
            WorkerLive,
            JobPluginsLive(
                pruner({ every: "10 millis", olderThan: "0 millis" }),
                rescuer({ every: "10 millis", rescueAfter: "0 millis" }),
            ),
        );

        const result = await runPromise(
            Effect.gen(function* () {
                const prune = yield* PruneJob.enqueue({ message: "prune" });
                const rescue = yield* RescueJob.enqueue({ message: "rescue" });
                yield* Jobs.run.pipe(Effect.timeoutOption("90 millis"));
                const pruned = yield* Job.find(prune.id);
                const rescued = yield* Job.find(rescue.id);

                return { pruned, rescued };
            }).pipe(
                Effect.provide(Live),
                Effect.provide(JobRuntimeLive(Jobs)),
                Effect.provide(JobRegistryMemory),
            ),
        );

        expect(Option.isNone(result.pruned)).toBe(true);
        expect(Option.isSome(result.rescued)).toBe(true);

        if (Option.isSome(result.rescued)) {
            expect(result.rescued.value.status).toBe("available");
        }
    });
});
