import { Effect, Layer, Schema } from "effect";

import { Job } from "../../src";

export const EchoJob = Job.make({
    name: "dev.echo",
    queue: "dev",
    payload: Schema.Struct({
        message: Schema.String,
    }),
});

export const EchoJobLive = EchoJob.toLayer((payload) =>
    Effect.gen(function* () {
        yield* Effect.logInfo(`processed dev.echo: ${payload.message}`);
    }),
);

export const JobsLive = Layer.mergeAll(EchoJobLive);
