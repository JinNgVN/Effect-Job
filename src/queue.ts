import type { Duration } from "effect";

import type { QueueName } from "./model";

export interface QueueDefinitionOptions {
    readonly concurrency?: number;
    readonly globalConcurrency?: number;
    readonly rateLimit?: {
        readonly limit: number;
        readonly per: Duration.Input;
    };
}

export interface QueueDefinition {
    readonly _tag: "QueueDefinition";
    readonly options: QueueDefinitionOptions;
}

export interface DynamicQueueName {
    readonly _tag: "DynamicQueueName";
    readonly name: QueueName;
}

export type QueueSelection<Queues extends string = string> =
    | Queues
    | DynamicQueueName;

export const Queue = {
    define: (options: QueueDefinitionOptions = {}): QueueDefinition => ({
        _tag: "QueueDefinition",
        options,
    }),
    dynamic: (name: QueueName): DynamicQueueName => ({
        _tag: "DynamicQueueName",
        name,
    }),
    isDynamic: (queue: unknown): queue is DynamicQueueName =>
        typeof queue === "object" &&
        queue !== null &&
        "_tag" in queue &&
        queue._tag === "DynamicQueueName",
};
