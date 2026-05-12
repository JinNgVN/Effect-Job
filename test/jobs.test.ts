import { Effect, Option, Schema } from "effect";
import { describe, expect, it } from "vitest";

import { effectJob, Job, JobCommand, JobEngine, memory, Queue } from "../src";

describe("Configured job insert API", () => {
    it("stores a job through the explicit command path", async () => {
        const Jobs = effectJob({
            queues: {
                mailers: {},
            },
        });
        const SendEmail = Jobs.define({
            name: "email.insert",
            queue: "mailers",
            payload: Schema.Struct({
                email: Schema.String,
            }),
            attempts: 5,
        });
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
            }).pipe(Effect.provide(memory())),
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

    it("supports pipeable command transformations", async () => {
        const Jobs = effectJob();
        const SyncAccount = Jobs.define({
            name: "account.sync",
            payload: Schema.Struct({
                accountId: Schema.String,
            }),
        });
        const record = await Jobs.runPromise(
            Effect.gen(function* () {
                const handle = yield* SyncAccount.command({ accountId: "acct_1" }).pipe(
                    JobCommand.withMeta({ source: "admin-panel" }),
                    JobCommand.withPriority(-5),
                    JobCommand.addTags(["manual"]),
                    Jobs.insert,
                );

                return yield* Job.find(handle.id);
            }).pipe(Effect.provide(memory())),
        );

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.priority).toBe(-5);
            expect(record.value.meta).toEqual({ source: "admin-panel" });
            expect(record.value.tags).toEqual(["manual"]);
        }
    });

    it("uses unique keys to return an existing active job", async () => {
        const Jobs = effectJob();
        const SendEmail = Jobs.define({
            name: "email.unique",
            payload: Schema.Struct({
                email: Schema.String,
            }),
            unique: {
                key: ({ payload }) => ["email", payload.email],
            },
        });
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
            }).pipe(Effect.provide(memory())),
        );

        expect(result.first.id).toBe(result.second.id);
        expect(result.records).toHaveLength(1);
        expect(result.records[0]?.idempotencyKey).toBe(
            "email:same@example.com",
        );
    });

    it("resolves dynamic queues explicitly", async () => {
        const Jobs = effectJob();
        const TenantJob = Jobs.define({
            name: "tenant.work",
            queue: ({ payload }) => Queue.dynamic(`tenant:${payload.tenantId}`),
            payload: Schema.Struct({
                tenantId: Schema.String,
            }),
        });
        const record = await Jobs.runPromise(
            Effect.gen(function* () {
                const handle = yield* TenantJob.enqueue({ tenantId: "tenant_1" });
                return yield* Job.find(handle.id);
            }).pipe(Effect.provide(memory())),
        );

        expect(Option.isSome(record)).toBe(true);

        if (Option.isSome(record)) {
            expect(record.value.queue).toBe("tenant:tenant_1");
        }
    });
});
