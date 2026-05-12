import { Effect, Layer, Option, Schema } from "effect";
import { describe, expect, it } from "vitest";

import { effectJob, JobRegistry, JobRegistryMemory } from "../src";

describe("effectJob define API", () => {
    it("defines runtime-bound jobs with an implicit default queue", () => {
        const Jobs = effectJob();
        const SendEmail = Jobs.define({
            name: "email.send",
            payload: Schema.Struct({
                email: Schema.String,
            }),
        });

        const command = SendEmail.command({ email: "person@example.com" });

        expect(SendEmail.name).toBe("email.send");
        expect(SendEmail.defaultMaxAttempts).toBe(20);
        expect(command.name).toBe("email.send");
        expect(command.payload).toEqual({ email: "person@example.com" });
        expect(command.valid).toBe(true);
    });

    it("registers a handler through job.toLayer", async () => {
        const Jobs = effectJob();
        const SendEmail = Jobs.define({
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
});
