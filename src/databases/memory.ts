import { JobEngineMemory } from "../engine";
import { JobNotifierMemory } from "../notifier";
import { Layer } from "effect";

export const memory = () => Layer.mergeAll(JobEngineMemory, JobNotifierMemory);
