// Compatibility alias for the vNext configured runtime. New code should use
// `JobSystem.memory`, `JobSystem.postgres`, or `JobSystem.custom` directly.

import type { Layer } from "effect";

import { JobCatalog, type JobCatalog as JobCatalogType } from "./catalog";
import type { JobDefinition } from "./job";
import { JobSystem, type ConfiguredJobSystem, type JobSystemBaseConfig } from "./system";

export interface EffectJobConfig<Catalog extends JobCatalogType<any> = JobCatalogType<any>>
    extends Omit<JobSystemBaseConfig<Catalog>, "catalog"> {
    readonly catalog?: Catalog;
    readonly database: Layer.Layer<any, any, any>;
    readonly jobs?: ReadonlyArray<JobDefinition.Any>;
}

export type ConfiguredEffectJob<Catalog extends JobCatalogType<any> = JobCatalogType<any>> =
    ConfiguredJobSystem<Catalog>;

export const effectJob = <Catalog extends JobCatalogType<any> = JobCatalogType<any>>(
    config: EffectJobConfig<Catalog>,
): ConfiguredEffectJob<Catalog> =>
    JobSystem.custom({
        ...config,
        catalog: config.catalog ?? (JobCatalog.define() as Catalog),
        database: config.database,
    });
