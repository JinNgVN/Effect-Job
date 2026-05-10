// Public package entrypoint.

import { Duration, Effect, Schema } from "effect";

export * from "./databases";
export * from "./effectJob";
export * from "./engine";
export * from "./job";
export * from "./model";
export * from "./notifier";
export * from "./plugin";
export * from "./plugins";
export * from "./registry";
export * from "./worker";

import { effectJob } from "./effectJob";
import { Job } from "./job";
import { memory } from "./databases";
import { pruner, rescuer } from "./plugins";

const shouldRunDemo =
    typeof Bun !== "undefined" && import.meta.path === Bun.main;

if (shouldRunDemo) {
    const CompleteJob = Job.make({
        name: "demo.complete",
        queue: "demo",
        payload: Schema.Struct({
            message: Schema.String,
        }),
    });

    const CancelJob = Job.make({
        name: "demo.cancel",
        queue: "demo",
        payload: Schema.Struct({
            reason: Schema.String,
        }),
        error: Schema.Unknown,
    });

    const SnoozeJob = Job.make({
        name: "demo.snooze",
        queue: "demo",
        payload: Schema.Struct({
            seconds: Schema.Number,
        }),
        error: Schema.Unknown,
    });

    const TimeoutJob = Job.make({
        name: "demo.timeout",
        queue: "demo",
        payload: Schema.Struct({
            message: Schema.String,
        }),
        attempts: 1,
        timeout: "10 millis",
    });

    const FailJob = Job.make({
        name: "demo.fail",
        queue: "demo",
        payload: Schema.Struct({
            message: Schema.String,
        }),
        error: Schema.String,
        attempts: 2,
        backoff: ({ attempt }) => Duration.millis(attempt * 25),
    });

    const jobs = effectJob({
        database: memory(),
        handlers: [
            CompleteJob.toLayer((payload, context) =>
                Effect.logInfo(
                    `${context.name} attempt ${context.attempt}: ${payload.message}`,
                ),
            ),
            CancelJob.toLayer((payload) => Job.cancel(payload.reason)),
            SnoozeJob.toLayer((payload) =>
                Job.snooze(`${payload.seconds} seconds`),
            ),
            TimeoutJob.toLayer(() => Effect.sleep("1 second")),
            FailJob.toLayer((payload) =>
                Effect.fail(`failed: ${payload.message}`),
            ),
        ],
        queues: {
            demo: { concurrency: 2, pollInterval: "10 millis" },
        },
        plugins: [
            pruner({
                every: "1 hour",
                olderThan: "7 days",
            }),
            rescuer(),
        ],
    });

    await CompleteJob.new({ message: "hello" }).pipe(jobs.insert);
    await CancelJob.new({ reason: "not needed anymore" }).pipe(jobs.insert);
    await SnoozeJob.new({ seconds: 60 }).pipe(jobs.insert);
    await TimeoutJob.new({ message: "too slow" }).pipe(jobs.insert);
    await FailJob.new({ message: "try again later" }).pipe(jobs.insert);

    await jobs.runPromise(
        jobs.worker().pipe(Effect.timeoutOption("150 millis")),
    );

    console.table(await jobs.queues());

    const records = await jobs.list();


    console.table(
        records.map((record) => ({
            name: record.name,
            status: record.status,
            attempt: record.attempt,
            maxAttempts: record.maxAttempts,
            runAt: record.runAt.toISOString(),
            completedAt: record.completedAt?.toISOString() ?? null,
            cancelledAt: record.cancelledAt?.toISOString() ?? null,
            discardedAt: record.discardedAt?.toISOString() ?? null,
            errors: record.errors
                .map((error) => `${error.kind}: ${error.message}`)
                .join("; "),
        })),
    );

    await jobs.dispose();
}
