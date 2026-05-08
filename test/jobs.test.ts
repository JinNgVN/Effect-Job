import { Effect, Option, Schema } from "effect";
import { describe, expect, it } from "vitest";

import { JobEngine, JobEngineMemory } from "../src/engine";
import { Job } from "../src/jobImpl";

describe("Job.enqueue", () => {
    it("stores a job in the engine", async () => {
        const job = Job.make({
            name: "demo.enqueue",
            queue: "demo",
            payload: Schema.Struct({
                message: Schema.String,
            }),
            success: Schema.Struct({
                ok: Schema.Boolean,
            }),
            attempts: 5,
        });

        const program = Effect.gen(function* () {
            const handle = yield* Job.enqueue(job, { message: "hello" });
            const engine = yield* JobEngine;
            const record = yield* engine.find(handle.id);

            return { handle, record };
        }).pipe(Effect.provide(JobEngineMemory));

        const result = await Effect.runPromise(program);

        expect(result.handle.name).toBe("demo.enqueue");
        expect(result.handle.queue).toBe("demo");
        expect(Option.isSome(result.record)).toBe(true);

        if (Option.isSome(result.record)) {
            expect(result.record.value.name).toBe("demo.enqueue");
            expect(result.record.value.queue).toBe("demo");
            expect(result.record.value.status).toBe("available");
            expect(result.record.value.attempt).toBe(0);
            expect(result.record.value.maxAttempts).toBe(5);
            expect(result.record.value.payload).toEqual({ message: "hello" });
        }
    });

    it("returns the existing active job for the same idempotency key", async () => {
        const job = Job.make({
            name: "demo.idempotent",
            payload: Schema.Struct({
                id: Schema.String,
            }),
            idempotencyKey: (payload) => payload.id,
        });

        const program = Effect.gen(function* () {
            const first = yield* Job.enqueue(job, { id: "same" });
            const second = yield* Job.enqueue(job, { id: "same" });
            const engine = yield* JobEngine;
            const records = yield* engine.list;

            return { first, second, records };
        }).pipe(Effect.provide(JobEngineMemory));

        const result = await Effect.runPromise(program);

        expect(result.first.id).toBe(result.second.id);
        expect(result.records).toHaveLength(1);
        expect(result.records[0]?.idempotencyKey).toBe("same");
    });

    it("can fail on duplicate idempotency keys", async () => {
        const job = Job.make({
            name: "demo.duplicate",
            payload: Schema.Struct({
                id: Schema.String,
            }),
            idempotencyKey: (payload) => payload.id,
        });

        const program = Effect.gen(function* () {
            yield* Job.enqueue(job, { id: "same" });
            return yield* Job.enqueue(job, { id: "same" }, { duplicate: "fail" }).pipe(
                Effect.as(undefined),
                Effect.catchTag("DuplicateJobError", (error) =>
                    Effect.succeed(error.existing),
                ),
            );
        }).pipe(
            Effect.provide(JobEngineMemory),
        );

        const existing = await Effect.runPromise(program);

        expect(existing).toBeDefined();

        if (existing !== undefined) {
            expect(existing.name).toBe("demo.duplicate");
            expect(existing.idempotencyKey).toBe("same");
        }
    });

    it("marks delayed jobs as scheduled", async () => {
        const job = Job.make({
            name: "demo.delayed",
            payload: Schema.Struct({
                message: Schema.String,
            }),
        });

        const program = Effect.gen(function* () {
            const handle = yield* Job.enqueue(
                job,
                { message: "later" },
                { delay: "1 hour" },
            );
            const engine = yield* JobEngine;
            return yield* engine.find(handle.id);
        }).pipe(Effect.provide(JobEngineMemory));

        const record = await Effect.runPromise(program);

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("scheduled");
        }
    });

    it("enqueues many jobs", async () => {
        const job = Job.make({
            name: "demo.bulk",
            queue: "bulk",
            payload: Schema.Struct({
                index: Schema.Number,
            }),
        });

        const program = Effect.gen(function* () {
            const handles = yield* Job.enqueueMany(job, [
                { index: 1 },
                { index: 2 },
                { index: 3 },
            ]);
            const engine = yield* JobEngine;
            const records = yield* engine.list;

            return { handles, records };
        }).pipe(Effect.provide(JobEngineMemory));

        const result = await Effect.runPromise(program);

        expect(result.handles).toHaveLength(3);
        expect(result.records).toHaveLength(3);
        expect(result.records.map((record) => record.payload)).toEqual([
            { index: 1 },
            { index: 2 },
            { index: 3 },
        ]);
        expect(result.records.every((record) => record.queue === "bulk")).toBe(
            true,
        );
    });

    it("applies idempotency during enqueueMany", async () => {
        const job = Job.make({
            name: "demo.bulk-idempotent",
            payload: Schema.Struct({
                id: Schema.String,
            }),
            idempotencyKey: (payload) => payload.id,
        });

        const program = Effect.gen(function* () {
            const handles = yield* Job.enqueueMany(job, [
                { id: "a" },
                { id: "a" },
                { id: "b" },
            ]);
            const engine = yield* JobEngine;
            const records = yield* engine.list;

            return { handles, records };
        }).pipe(Effect.provide(JobEngineMemory));

        const result = await Effect.runPromise(program);

        expect(result.handles).toHaveLength(3);
        expect(result.handles[0]?.id).toBe(result.handles[1]?.id);
        expect(result.records).toHaveLength(2);
        expect(result.records.map((record) => record.idempotencyKey)).toEqual([
            "a",
            "b",
        ]);
    });
});
