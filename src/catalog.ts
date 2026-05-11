import { Schema } from "effect";

import {
    makeJobDefinition,
    type JobDefinition,
    type JobDefinitionOptions,
} from "./job";
import { Queue, type QueueDefinition } from "./queue";

export type QueueDefinitions = Readonly<Record<string, QueueDefinition>>;

export type CatalogQueueNames<Queues extends QueueDefinitions> =
    | "default"
    | Extract<keyof Queues, string>;

export interface JobCatalogDefinition<Queues extends QueueDefinitions> {
    readonly queues?: Queues;
}

export interface ScheduleDefinition<Job extends JobDefinition.Any = JobDefinition.Any> {
    readonly cron: string;
    readonly timezone?: string;
    readonly job: Job;
    readonly payload: unknown;
    readonly enabled?: boolean;
    readonly unique?: { readonly key: "per-tick" | string };
}

export interface JobCatalog<Queues extends QueueDefinitions = QueueDefinitions> {
    readonly _tag: "JobCatalog";
    readonly queues: Queues & { readonly default: QueueDefinition };
    readonly queueNames: ReadonlyArray<CatalogQueueNames<Queues>>;
    readonly job: <
        const Name extends string,
        PayloadSchema extends Schema.Top,
        ResultSchema extends Schema.Top = typeof Schema.Unknown,
    >(
        options: JobDefinitionOptions<
            Name,
            PayloadSchema,
            ResultSchema,
            CatalogQueueNames<Queues>
        >,
    ) => JobDefinition<
        Name,
        PayloadSchema,
        ResultSchema,
        CatalogQueueNames<Queues>
    >;
    readonly schedules: <Schedules extends Readonly<Record<string, ScheduleDefinition>>>(
        schedules: Schedules,
    ) => Schedules;
}

export const JobCatalog = {
    define: <Queues extends QueueDefinitions = Record<never, never>>(
        definition: JobCatalogDefinition<Queues> = {},
    ): JobCatalog<Queues> => {
        const queues = {
            default: Queue.define(),
            ...(definition.queues ?? {}),
        } as Queues & { readonly default: QueueDefinition };

        return {
            _tag: "JobCatalog",
            queues,
            queueNames: Object.keys(queues) as ReadonlyArray<
                CatalogQueueNames<Queues>
            >,
            job: (options) => makeJobDefinition(options as any) as any,
            schedules: (schedules) => schedules,
        };
    },
};
