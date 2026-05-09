// Periodic rescue for jobs stuck executing after worker shutdown.

import { Duration, Effect } from "effect";

import { Job } from "../job";
import type { EffectJobPlugin } from "../plugin";

export interface RescuerOptions {
    readonly every?: Duration.Input;
    readonly rescueAfter?: Duration.Input;
}

const cutoff = (rescueAfter: Duration.Input): Date =>
    new Date(Date.now() - Duration.toMillis(rescueAfter));

export const rescuer = (options: RescuerOptions = {}): EffectJobPlugin => {
    const every = options.every ?? "1 minute";
    const rescueAfter = options.rescueAfter ?? "60 minutes";

    return {
        name: "rescuer",
        onWorkerStarted: () =>
            Effect.gen(function* () {
                const rescueOnce = Effect.gen(function* () {
                    const result = yield* Job.rescueExecuting({
                        before: cutoff(rescueAfter),
                    });
                    const rescuedCount = result.rescued.length;
                    const discardedCount = result.discarded.length;

                    if (rescuedCount > 0 || discardedCount > 0) {
                        yield* Effect.logInfo(
                            `Rescued ${rescuedCount} jobs and discarded ${discardedCount} jobs`,
                        );
                    }
                });

                // The rescuer is scoped to the worker, so it stops when the worker stops.
                yield* Effect.forkScoped(
                    Effect.forever(rescueOnce.pipe(Effect.delay(every))),
                );
            }),
    };
};
