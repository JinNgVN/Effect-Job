import {
    makeConfiguredSystem,
    type EffectJobRuntime,
    type JobRuntimeConfig,
    type JobRuntimeQueues,
} from "./system";

export interface EffectJobConfig<
    Queues extends JobRuntimeQueues = JobRuntimeQueues,
> extends JobRuntimeConfig<Queues> {}

export type ConfiguredEffectJob<
    Queues extends JobRuntimeQueues = JobRuntimeQueues,
> = EffectJobRuntime<Queues>;

export const effectJob = <Queues extends JobRuntimeQueues = JobRuntimeQueues>(
    config: EffectJobConfig<Queues> = {} as EffectJobConfig<Queues>,
): ConfiguredEffectJob<Queues> =>
    makeConfiguredSystem(config);
