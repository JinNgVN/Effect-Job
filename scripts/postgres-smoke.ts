import { PgClient } from "@effect/sql-pg";
import { existsSync, readFileSync } from "node:fs";
import { Effect, Layer, Option, Redacted, Schema } from "effect";

import {
    effectJob,
    Job,
    JobEngine,
    JobEnginePostgres,
    JobNotifierMemory,
} from "../src/index";


const databaseUrl = process.env.DATABASE_URL;

if (databaseUrl === undefined || databaseUrl.length === 0) {
    throw new Error("DATABASE_URL is required for scripts/postgres-smoke.ts");
}

const Jobs = effectJob({
    queues: {
        smoke: { concurrency: 1, pollInterval: "100 millis" },
    },
});

const SmokeJob = Jobs.define({
    name: "smoke.postgres",
    queue: "smoke",
    payload: Schema.Struct({
        message: Schema.String,
    }),
});

const Live = Layer.mergeAll(

    JobEnginePostgres.layer({
        schema: "public",
        table: "effect_jobs",
    }),
    JobNotifierMemory,
).pipe(Layer.provideMerge(PgClient.layer({
    url: Redacted.make(databaseUrl),
})));

const expectSome = <A>(option: Option.Option<A>, label: string) =>
    Option.match(option, {
        onNone: () => Effect.fail(new Error(`Expected ${label}`)),
        onSome: Effect.succeed,
    });

const program = Effect.gen(function* () {
    const handle = yield* SmokeJob.enqueue({
        message: `hello from postgres smoke ${Date.now()}`,
    });
    const inserted = yield* expectSome(
        yield* Job.find(handle.id),
        "inserted job",
    );
    const engine = yield* JobEngine;
    const claimed = yield* expectSome(
        yield* engine.claimNext({
            queue: "smoke",
            workerId: "postgres-smoke",
        }),
        "claimed job",
    );

    yield* engine.complete(claimed.id);

    const completed = yield* expectSome(
        yield* Job.find(claimed.id),
        "completed job",
    );

    if (completed.status !== "completed") {
        return yield* Effect.fail(
            new Error(`Expected completed status, received ${completed.status}`),
        );
    }

    if (completed.attempt !== 0 || completed.executions !== 1) {
        return yield* Effect.fail(
            new Error(
                `Expected attempt=0 and executions=1, received attempt=${completed.attempt}, executions=${completed.executions}`,
            ),
        );
    }

    yield* Effect.logInfo("Postgres smoke passed", {
        id: completed.id,
        insertedStatus: inserted.status,
        completedStatus: completed.status,
        attempt: completed.attempt,
        executions: completed.executions,
        snoozes: completed.snoozes,
    });
});

await Jobs.runPromise(program.pipe(Effect.provide(Live)));
await Jobs.dispose();
