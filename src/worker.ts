import { Data, Effect, Option, Schema } from "effect";

import { JobEngine, type JobRecord } from "./engine";
import type { JobName, QueueName } from "./job";
import { JobRegistry } from "./registry";

export class MissingJobHandlerError extends Data.TaggedError(
    "MissingJobHandlerError",
)<{
    readonly jobName: JobName;
}> {}

export interface WorkerRunOptions {
    readonly queue?: QueueName;
}

export const Worker = {
    runOnce: (
        options?: WorkerRunOptions,
    ): Effect.Effect<
        Option.Option<JobRecord>,
        MissingJobHandlerError | Schema.SchemaError | unknown,
        JobEngine | JobRegistry
    > =>
        Effect.gen(function* () {
            const engine = yield* JobEngine;
            const registry = yield* JobRegistry;
            const record = yield* engine.claimNext(options);

            if (Option.isNone(record)) {
                return Option.none();
            }

            const registeredJob = yield* registry.get(record.value.name);

            if (Option.isNone(registeredJob)) {
                return yield* new MissingJobHandlerError({
                    jobName: record.value.name,
                });
            }

            const payload = yield* Schema.decodeUnknownEffect(
                registeredJob.value.job.payloadSchema,
            )(record.value.payload);

            yield* Effect.logInfo("Working");
            yield* registeredJob.value.run(payload);
            yield* engine.complete(record.value.id);

            return record;
        }),
};
