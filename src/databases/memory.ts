import { JobEngineMemory } from "../engine";

export const memory = () => JobEngineMemory;

export const JobEngineMemoryLive = JobEngineMemory;

export const JobEngineMemoryLayer = {
    layer: memory,
};
