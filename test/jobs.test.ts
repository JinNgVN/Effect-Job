import { Effect, Option, Schema } from "effect";
import { describe, expect, it } from "vitest";

import {
    Job,
    JobCatalog,
    JobCommand,
    JobEngine,
    JobSystem,
    Queue,
} from "../src";

describe("JobSystem insert API", () => {
    it("stores a job through the explicit command path", async () => {
        const Catalog = JobCatalog.define({
            queues: {
                mailers: Queue.define(),
            },
        });
        const SendEmail = Catalog.job({
            name: "email.insert",
            queue: "mailers",
            payload: Schema.Struct({
                email: Schema.String,
            }),
            attempts: 5,
        });
        const Jobs = JobSystem.memory({ catalog: Catalog, jobs: [SendEmail] });
        const result = await Jobs.runPromise(
            Effect.gen(function* () {
                const handle = yield* Jobs.insert(
                    SendEmail.command(
                        { email: "person@example.com" },
                        {
                            meta: { tenantId: "tenant_1" },
                            tags: ["welcome", "email"],
                        },
                    ),
                );
                const record = yield* Job.find(handle.id);

                return { handle, record };
            }),
        );

        expect(result.handle.name).toBe("email.insert");
        expect(result.handle.queue).toBe("mailers");
        expect(Option.isSome(result.record)).toBe(true);

        if (Option.isSome(result.record)) {
            expect(result.record.value.status).toBe("available");
            expect(result.record.value.maxAttempts).toBe(5);
            expect(result.record.value.payload).toEqual({
                email: "person@example.com",
            });
            expect(result.record.value.meta).toEqual({ tenantId: "tenant_1" });
            expect(result.record.value.tags).toEqual(["welcome", "email"]);
        }
    });

    it("supports pipeable Oban-style command transformations", async () => {
        const Catalog = JobCatalog.define();
        const SyncAccount = Catalog.job({
            name: "account.sync",
            payload: Schema.Struct({
                accountId: Schema.String,
            }),
        });
        const Jobs = JobSystem.memory({ catalog: Catalog, jobs: [SyncAccount] });
        const record = await Jobs.runPromise(
            Effect.gen(function* () {
                const handle = yield* SyncAccount.command({ accountId: "acct_1" }).pipe(
                    JobCommand.withMeta({ source: "admin-panel" }),
                    JobCommand.withPriority(-5),
                    JobCommand.addTags(["manual"]),
                    Jobs.insert,
                );

                return yield* Job.find(handle.id);
            }),
        );

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.priority).toBe(-5);
            expect(record.value.meta).toEqual({ source: "admin-panel" });
            expect(record.value.tags).toEqual(["manual"]);
        }
    });

    it("uses unique keys to return an existing active job", async () => {
        const Catalog = JobCatalog.define();
        const SendEmail = Catalog.job({
            name: "email.unique",
            payload: Schema.Struct({
                email: Schema.String,
            }),
            unique: {
                key: ({ payload }) => ["email", payload.email],
            },
        });
        const Jobs = JobSystem.memory({ catalog: Catalog, jobs: [SendEmail] });
        const result = await Jobs.runPromise(
            Effect.gen(function* () {
                const first = yield* SendEmail.enqueue({
                    email: "same@example.com",
                });
                const second = yield* SendEmail.enqueue({
                    email: "same@example.com",
                });
                const engine = yield* JobEngine;
                const records = yield* engine.list();

                return { first, second, records };
            }),
        );

        expect(result.first.id).toBe(result.second.id);
        expect(result.records).toHaveLength(1);
        expect(result.records[0]?.idempotencyKey).toBe(
            "email:same@example.com",
        );
    });

    it("can fail on duplicate unique keys", async () => {
        const Catalog = JobCatalog.define();
        const SendEmail = Catalog.job({
            name: "email.duplicate",
            payload: Schema.Struct({
                email: Schema.String,
            }),
            unique: {
                key: ({ payload }) => payload.email,
            },
        });
        const Jobs = JobSystem.memory({ catalog: Catalog, jobs: [SendEmail] });
        const existing = await Jobs.runPromise(
            Effect.gen(function* () {
                yield* SendEmail.enqueue({ email: "same@example.com" });
                return yield* SendEmail.enqueue(
                    { email: "same@example.com" },
                    { duplicate: "fail" },
                ).pipe(
                    Effect.as(undefined),
                    Effect.catchTag("DuplicateJobError", (error) =>
                        Effect.succeed(error.existing),
                    ),
                );
            }),
        );

        expect(existing).toBeDefined();
        expect(existing?.idempotencyKey).toBe("same@example.com");
    });

    it("resolves dynamic queues explicitly", async () => {
        const Catalog = JobCatalog.define();
        const TenantJob = Catalog.job({
            name: "tenant.work",
            queue: ({ payload }) => Queue.dynamic(`tenant:${payload.tenantId}`),
            payload: Schema.Struct({
                tenantId: Schema.String,
            }),
        });
        const Jobs = JobSystem.memory({ catalog: Catalog, jobs: [TenantJob] });
        const record = await Jobs.runPromise(
            Effect.gen(function* () {
                const handle = yield* TenantJob.enqueue({ tenantId: "tenant_1" });
                return yield* Job.find(handle.id);
            }),
        );

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.queue).toBe("tenant:tenant_1");
        }
    });

    it("supports bulk enqueue and delayed jobs", async () => {
        const Catalog = JobCatalog.define({
            queues: {
                bulk: Queue.define(),
            },
        });
        const BulkJob = Catalog.job({
            name: "bulk.insert",
            queue: "bulk",
            payload: Schema.Struct({
                index: Schema.Number,
            }),
        });
        const Jobs = JobSystem.memory({ catalog: Catalog, jobs: [BulkJob] });
        const result = await Jobs.runPromise(
            Effect.gen(function* () {
                const handles = yield* BulkJob.enqueueMany([
                    { index: 1 },
                    { index: 2 },
                    { index: 3 },
                ]);
                const delayed = yield* BulkJob.enqueue(
                    { index: 4 },
                    { delay: "1 hour" },
                );
                const records = yield* Job.list();

                return { handles, delayed, records };
            }),
        );

        expect(result.handles).toHaveLength(3);
        expect(result.records).toHaveLength(4);
        expect(
            result.records.find((record) => record.id === result.delayed.id)
                ?.status,
        ).toBe("scheduled");
        expect(result.records.every((record) => record.queue === "bulk")).toBe(
            true,
        );
    });

    it("keeps external job controls on the configured system", async () => {
        const Catalog = JobCatalog.define();
        const DemoJob = Catalog.job({
            name: "demo.controls",
            payload: Schema.Struct({
                id: Schema.String,
            }),
        });
        const Jobs = JobSystem.memory({ catalog: Catalog, jobs: [DemoJob] });
        const record = await Jobs.runPromise(
            Effect.gen(function* () {
                const handle = yield* DemoJob.enqueue(
                    { id: "a" },
                    { delay: "1 hour" },
                );
                yield* Jobs.jobs.retry(handle.id);
                yield* Jobs.jobs.cancel(handle.id, { reason: "admin cancelled" });
                return yield* Job.find(handle.id);
            }),
        );

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.status).toBe("cancelled");
            expect(record.value.cancelledAt).toBeInstanceOf(Date);
        }
    });
});
