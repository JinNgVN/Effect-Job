import { Effect, Layer, Option, Schema } from "effect";
import { describe, expect, it } from "vitest";

import { JobEngine, JobEngineMemory } from "../src/engine";
import { Job } from "../src/jobImpl";
import { JobRegistryMemory } from "../src/registry";
import { Worker } from "../src/worker";

describe("Worker.runOnce", () => {
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
            const handle = yield* Job.enqueue(job, { message: "hello" });
            const claimed = yield* Worker.runOnce();
            const engine = yield* JobEngine;
            const record = yield* engine.find(handle.id);

            return { claimed, record };
        }).pipe(Effect.provide(AppLive));

        const result = await Effect.runPromise(program);

        expect(handled).toBe("hello");
        expect(Option.isSome(result.claimed)).toBe(true);
        expect(Option.isSome(result.record)).toBe(true);

        if (Option.isSome(result.record)) {
            expect(result.record.value.status).toBe("completed");
            expect(result.record.value.attempt).toBe(1);
        }
    });

    it("returns none when no job is available", async () => {
        const program = Worker.runOnce().pipe(
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
            yield* Job.enqueue(mailJob, { id: "mail" });
            yield* Job.enqueue(billingJob, { id: "billing" });
            const claimed = yield* Worker.runOnce({ queue: "billing" });
            const engine = yield* JobEngine;
            const records = yield* engine.list;

            return { claimed, records };
        }).pipe(Effect.provide(AppLive));

        const result = await Effect.runPromise(program);

        expect(Option.isSome(result.claimed)).toBe(true);

        if (Option.isSome(result.claimed)) {
            expect(result.claimed.value.name).toBe("demo.billing");
        }

        expect(
            result.records.find((record) => record.name === "demo.mail")
                ?.status,
        ).toBe("available");
        expect(
            result.records.find((record) => record.name === "demo.billing")
                ?.status,
        ).toBe("completed");
    });
});
