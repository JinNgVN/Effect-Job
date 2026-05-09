// Wake-up signals for workers. Storage remains the source of truth; notifications only say "look again".

import { Context, Effect, Layer, PubSub } from "effect";

import type { QueueName } from "./model";

export interface JobInsertNotification {
    readonly queue: QueueName;
}

export interface JobNotifierShape {
    readonly notifyInsert: (
        notification: JobInsertNotification,
    ) => Effect.Effect<void>;
    readonly waitForInsert: (options?: {
        readonly queue?: QueueName;
    }) => Effect.Effect<void>;
}

export const JobNotifier = Context.Reference<JobNotifierShape>(
    "effect-job/JobNotifier",
    {
        defaultValue: () => ({
            notifyInsert: () => Effect.void,
            waitForInsert: () => Effect.never,
        }),
    },
);

const waitsForQueue = (
    notification: JobInsertNotification,
    queue: QueueName | undefined,
): boolean => queue === undefined || notification.queue === queue;

export const JobNotifierMemory = Layer.effect(
    JobNotifier,
    Effect.gen(function* () {
        const pubsub = yield* PubSub.sliding<JobInsertNotification>(1024);

        return {
            notifyInsert: (notification) =>
                PubSub.publish(pubsub, notification).pipe(Effect.asVoid),
            waitForInsert: (options) =>
                Effect.scoped(
                    Effect.gen(function* () {
                        const subscription = yield* PubSub.subscribe(pubsub);

                        while (true) {
                            const notification = yield* PubSub.take(
                                subscription,
                            );

                            if (waitsForQueue(notification, options?.queue)) {
                                return;
                            }
                        }
                    }),
                ),
        };
    }),
);
