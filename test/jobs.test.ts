import { Effect, Option, Schema } from "effect";
import { describe, expect, it } from "vitest";

import { Job, JobEngine, JobEngineMemory } from "../src";

describe("Job.insert", () => {
    it("stores a job in the engine", async () => {
        const job = Job.make({
            name: "demo.insert-now",
            queue: "demo",
            payload: Schema.Struct({
                message: Schema.String,
            }),
            attempts: 5,
        });

        const program = Effect.gen(function* () {
            const handle = yield* job.new(
                { message: "hello" },
                {
                    meta: { tenantId: "tenant_1" },
                    tags: ["welcome", "email"],
                },
            ).pipe(Job.insert);
            const engine = yield* JobEngine;
            const record = yield* engine.find(handle.id);

            return { handle, record };
        }).pipe(Effect.provide(JobEngineMemory));

        const result = await Effect.runPromise(program);

        expect(result.handle.name).toBe("demo.insert-now");
        expect(result.handle.queue).toBe("demo");
        expect(Option.isSome(result.record)).toBe(true);

        if (Option.isSome(result.record)) {
            expect(result.record.value.name).toBe("demo.insert-now");
            expect(result.record.value.queue).toBe("demo");
            expect(result.record.value.status).toBe("available");
            expect(result.record.value.attempt).toBe(0);
            expect(result.record.value.maxAttempts).toBe(5);
            expect(result.record.value.payload).toEqual({ message: "hello" });
            expect(result.record.value.meta).toEqual({ tenantId: "tenant_1" });
            expect(result.record.value.tags).toEqual(["welcome", "email"]);
            expect(result.record.value.attemptedBy).toEqual([]);
        }
    });

    it("can create a job insert with job.new and insert it later", async () => {
        const job = Job.make({
            name: "demo.insert",
            queue: "mailers",
            payload: Schema.Struct({
                to: Schema.String,
            }),
            idempotencyKey: (payload) => payload.to,
        });

        const program = Effect.gen(function* () {
            const prepared = yield* job.new(
                { to: "foo@example.com" },
                {
                    meta: { source: "signup" },
                    tags: ["welcome"],
                    duplicate: "fail",
                },
            );
            const handle = yield* Job.insert(prepared);
            const record = yield* Job.find(handle.id);

            return { prepared, handle, record };
        }).pipe(Effect.provide(JobEngineMemory));

        const result = await Effect.runPromise(program);

        expect(result.prepared.newJob.name).toBe("demo.insert");
        expect(result.prepared.newJob.idempotencyKey).toBe(
            "foo@example.com",
        );
        expect(result.handle.queue).toBe("mailers");
        expect(Option.isSome(result.record)).toBe(true);

        if (Option.isSome(result.record)) {
            expect(result.record.value.meta).toEqual({ source: "signup" });
            expect(result.record.value.tags).toEqual(["welcome"]);
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
            const first = yield* job.new({ id: "same" }).pipe(Job.insert);
            const second = yield* job.new({ id: "same" }).pipe(Job.insert);
            const engine = yield* JobEngine;
            const records = yield* engine.list();

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
            yield* job.new({ id: "same" }).pipe(Job.insert);
            return yield* job.new({ id: "same" }, { duplicate: "fail" }).pipe(
                Job.insert,
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
            const handle = yield* job.new(
                { message: "later" },
                { delay: "1 hour" },
            ).pipe(Job.insert);
            const engine = yield* JobEngine;
            return yield* engine.find(handle.id);
        }).pipe(Effect.provide(JobEngineMemory));

        const record = await Effect.runPromise(program);

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("scheduled");
        }
    });

    it("inserts many jobs", async () => {
        const job = Job.make({
            name: "demo.bulk",
            queue: "bulk",
            payload: Schema.Struct({
                index: Schema.Number,
            }),
        });

        const program = Effect.gen(function* () {
            const handles = yield* Job.insertMany([
                { index: 1 },
                { index: 2 },
                { index: 3 },
            ].map((payload) => job.new(payload)));
            const engine = yield* JobEngine;
            const records = yield* engine.list();

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

    it("applies idempotency during insertMany", async () => {
        const job = Job.make({
            name: "demo.bulk-idempotent",
            payload: Schema.Struct({
                id: Schema.String,
            }),
            idempotencyKey: (payload) => payload.id,
        });

        const program = Effect.gen(function* () {
            const handles = yield* Job.insertMany([
                { id: "a" },
                { id: "a" },
                { id: "b" },
            ].map((payload) => job.new(payload)));
            const engine = yield* JobEngine;
            const records = yield* engine.list();

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

    it("can cancel a job externally by id", async () => {
        const job = Job.make({
            name: "demo.external-cancel",
            payload: Schema.Struct({
                id: Schema.String,
            }),
        });

        const program = Effect.gen(function* () {
            const handle = yield* job.new({ id: "a" }).pipe(Job.insert);
            yield* Job.cancelById(handle.id, "admin cancelled");
            return yield* Job.find(handle.id);
        }).pipe(Effect.provide(JobEngineMemory));

        const record = await Effect.runPromise(program);

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("cancelled");
            expect(record.value.cancelledAt).toBeInstanceOf(Date);
            expect(record.value.errors[0]?.message).toBe("admin cancelled");
        }
    });

    it("can make scheduled jobs available immediately", async () => {
        const job = Job.make({
            name: "demo.run-now",
            payload: Schema.Struct({
                id: Schema.String,
            }),
        });

        const program = Effect.gen(function* () {
            const handle = yield* job.new({ id: "a" }, { delay: "1 hour" }).pipe(Job.insert);
            yield* Job.runNow(handle.id);
            return yield* Job.find(handle.id);
        }).pipe(Effect.provide(JobEngineMemory));

        const record = await Effect.runPromise(program);

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("available");
        }
    });

    it("lists jobs by queue and status", async () => {
        const mailJob = Job.make({
            name: "demo.list.mail",
            queue: "mailers",
            payload: Schema.Struct({
                id: Schema.String,
            }),
        });
        const billingJob = Job.make({
            name: "demo.list.billing",
            queue: "billing",
            payload: Schema.Struct({
                id: Schema.String,
            }),
        });

        const program = Effect.gen(function* () {
            yield* mailJob.new({ id: "mail" }).pipe(Job.insert);
            yield* billingJob.new({ id: "billing" }, { delay: "1 hour" }).pipe(Job.insert);
            const mailers = yield* Job.list({ queue: "mailers" });
            const scheduled = yield* Job.list({ status: "scheduled" });

            return { mailers, scheduled };
        }).pipe(Effect.provide(JobEngineMemory));

        const result = await Effect.runPromise(program);

        expect(result.mailers.map((record) => record.name)).toEqual([
            "demo.list.mail",
        ]);
        expect(result.scheduled.map((record) => record.name)).toEqual([
            "demo.list.billing",
        ]);
    });

    it("can list by multiple queues and statuses with a limit", async () => {
        const job = Job.make({
            name: "demo.list.multi",
            payload: Schema.Struct({
                id: Schema.String,
            }),
        });

        const program = Effect.gen(function* () {
            yield* job.new({ id: "one" }).pipe(Job.insert);
            const scheduled = yield* job.new({ id: "two" }, { delay: "1 hour" }).pipe(Job.insert);
            yield* Job.cancelById(scheduled.id, "cancelled");

            return yield* Job.list({
                queue: ["default"],
                status: ["available", "cancelled"],
                limit: 1,
            });
        }).pipe(Effect.provide(JobEngineMemory));

        const records = await Effect.runPromise(program);

        expect(records).toHaveLength(1);
        expect(["available", "cancelled"]).toContain(records[0]?.status);
    });

    it("prunes terminal jobs before a cutoff", async () => {
        const job = Job.make({
            name: "demo.prune",
            payload: Schema.Struct({
                id: Schema.String,
            }),
        });

        const program = Effect.gen(function* () {
            const terminal = yield* job.new({ id: "terminal" }).pipe(Job.insert);
            const active = yield* job.new({ id: "active" }).pipe(Job.insert);
            yield* Job.cancelById(terminal.id, "done");

            const deleted = yield* Job.prune({
                before: new Date(Date.now() + 1),
            });
            const records = yield* Job.list();

            return { active, deleted, records };
        }).pipe(Effect.provide(JobEngineMemory));

        const result = await Effect.runPromise(program);

        expect(result.deleted).toBe(1);
        expect(result.records.map((record) => record.id)).toEqual([
            result.active.id,
        ]);
    });

    it("rescues stale executing jobs back to available", async () => {
        const job = Job.make({
            name: "demo.rescue",
            payload: Schema.Struct({
                id: Schema.String,
            }),
        });

        const program = Effect.gen(function* () {
            const handle = yield* job.new({ id: "stale" }).pipe(Job.insert);
            const engine = yield* JobEngine;
            const claimed = yield* engine.claimNext({
                workerId: "worker-rescue",
            });

            const result = yield* Job.rescueExecuting({
                before: new Date(Date.now() + 1),
            });
            const record = yield* Job.find(handle.id);

            return { claimed, result, record };
        }).pipe(Effect.provide(JobEngineMemory));

        const result = await Effect.runPromise(program);

        expect(Option.isSome(result.claimed)).toBe(true);
        expect(result.result.rescued).toHaveLength(1);
        expect(result.result.discarded).toHaveLength(0);
        expect(Option.isSome(result.record)).toBe(true);

        if (Option.isSome(result.record)) {
            expect(result.record.value.status).toBe("available");
            expect(result.record.value.attempt).toBe(1);
            expect(result.record.value.attemptedBy).toEqual([
                "worker-rescue",
            ]);
        }
    });

    it("discards stale executing jobs with exhausted attempts", async () => {
        const job = Job.make({
            name: "demo.rescue-exhausted",
            payload: Schema.Struct({
                id: Schema.String,
            }),
            attempts: 1,
        });

        const program = Effect.gen(function* () {
            const handle = yield* job.new({ id: "exhausted" }).pipe(Job.insert);
            const engine = yield* JobEngine;
            yield* engine.claimNext({ workerId: "worker-rescue" });

            const result = yield* Job.rescueExecuting({
                before: new Date(Date.now() + 1),
            });
            const record = yield* Job.find(handle.id);

            return { result, record };
        }).pipe(Effect.provide(JobEngineMemory));

        const result = await Effect.runPromise(program);

        expect(result.result.rescued).toHaveLength(0);
        expect(result.result.discarded).toHaveLength(1);
        expect(Option.isSome(result.record)).toBe(true);

        if (Option.isSome(result.record)) {
            expect(result.record.value.status).toBe("discarded");
            expect(result.record.value.discardedAt).toBeInstanceOf(Date);
        }
    });
});
