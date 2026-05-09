// Periodic pruning for terminal jobs.

import { Duration, Effect } from "effect";

import { Job } from "../job";
import type { JobStatus } from "../model";
import type { EffectJobPlugin } from "../plugin";

export interface PrunerOptions {
    readonly every: Duration.Input;
    readonly olderThan: Duration.Input;
    readonly statuses?: ReadonlyArray<JobStatus>;
}

const cutoff = (olderThan: Duration.Input): Date =>
    new Date(Date.now() - Duration.toMillis(olderThan));

export const pruner = (options: PrunerOptions): EffectJobPlugin => ({
    name: "pruner",
    onWorkerStarted: () =>
        Effect.gen(function* () {
            const pruneOnce = Effect.gen(function* () {
                const deleted = yield* Job.prune({
                    before: cutoff(options.olderThan),
                    statuses: options.statuses,
                });

                if (deleted > 0) {
                    yield* Effect.logInfo(`Pruned ${deleted} jobs`);
                }
            });

            // The pruner is scoped to the worker, so it stops when the worker stops.
            yield* Effect.forkScoped(
                Effect.forever(pruneOnce.pipe(Effect.delay(options.every))),
            );
        }),
});
