import { Effect, Layer, Option, Schema } from "effect";
import { describe, expect, it } from "vitest";

import { JobCatalog, JobRegistry, JobRegistryMemory, Queue } from "../src";

describe("JobCatalog", () => {
    it("creates runtime-independent jobs with an implicit default queue", () => {
        const Catalog = JobCatalog.define();
        const SendEmail = Catalog.job({
            name: "email.send",
            payload: Schema.Struct({
                email: Schema.String,
            }),
        });

        const command = SendEmail.command({ email: "person@example.com" });

        expect(Catalog.queueNames).toEqual(["default"]);
        expect(SendEmail.name).toBe("email.send");
        expect(SendEmail.defaultMaxAttempts).toBe(20);
        expect(command.name).toBe("email.send");
        expect(command.payload).toEqual({ email: "person@example.com" });
        expect(command.valid).toBe(true);
    });

    it("allows the implicit default queue to be overridden", () => {
        const Catalog = JobCatalog.define({
            queues: {
                default: Queue.define({ concurrency: 5 }),
                mailers: Queue.define(),
            },
        });

        expect(Catalog.queues.default.options.concurrency).toBe(5);
        expect([...Catalog.queueNames].sort()).toEqual(["default", "mailers"]);
    });

    it("registers a handler through job.toLayer", async () => {
        const Catalog = JobCatalog.define();
        const SendEmail = Catalog.job({
            name: "email.layer",
            payload: Schema.Struct({
                email: Schema.String,
            }),
        });
        const jobLayer = SendEmail.toLayer(({ payload }) =>
            Effect.succeed({ ok: payload.email.includes("@") }),
        );
        const program = Effect.gen(function* () {
            const registry = yield* JobRegistry;
            const handler = yield* registry.get("email.layer");
            const handlers = yield* registry.list;

            return { handler, handlers };
        }).pipe(Effect.provide(jobLayer.pipe(Layer.provide(JobRegistryMemory))));

        const result = await Effect.runPromise(program as Effect.Effect<any, any, never>);

        expect(Option.isSome(result.handler)).toBe(true);
        expect(result.handlers).toHaveLength(1);
        expect(result.handlers[0]?.job.name).toBe("email.layer");
    });

    it("fails fast on duplicate handler registration", async () => {
        const Catalog = JobCatalog.define();
        const SendEmail = Catalog.job({
            name: "email.duplicate",
            payload: Schema.Struct({
                email: Schema.String,
            }),
        });
        const first = SendEmail.toLayer(() => Effect.void);
        const second = SendEmail.toLayer(() => Effect.void);
        const program = Effect.gen(function* () {
            const registry = yield* JobRegistry;
            return yield* registry.list;
        }).pipe(
            Effect.provide(
                Layer.mergeAll(first, second).pipe(Layer.provide(JobRegistryMemory)),
            ),
        );

        await expect(
            Effect.runPromise(program as Effect.Effect<any, any, never>),
        ).rejects.toMatchObject({
            _tag: "DuplicateJobHandlerError",
            jobName: "email.duplicate",
        });
    });
});
