import { Effect, Schema } from "effect";

import { JobCatalog, Queue } from "../../src";

export const Catalog = JobCatalog.define({
    queues: {
        dev: Queue.define(),
    },
});

export const EchoJob = Catalog.job({
    name: "dev.echo",
    queue: "dev",
    payload: Schema.Struct({
        message: Schema.String,
    }),
    run: ({ payload }) =>
        Effect.gen(function* () {
            yield* Effect.logInfo(`processed dev.echo: ${payload.message}`);
        }),
});
