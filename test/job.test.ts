import { Effect, Layer, Option, Schema } from "effect"
import { describe, expect, it } from "vitest"
import { Job, JobRegistry, JobRegistryMemory } from "../src"

describe("Job.make", () => {
  it("creates a job definition with defaults", () => {
    const job = Job.make({
      name: "demo.hello",
      payload: Schema.Struct({
        message: Schema.String
      })
    })

    expect(job.name).toBe("demo.hello")
    expect(job.queue).toBe("default")
    expect(job.attempts).toBe(20)
    expect(job.errorSchema).toBe(Schema.Never)
    expect(typeof job.toLayer).toBe("function")
  })

  it("registers a handler through toLayer", async () => {
    const job = Job.make({
      name: "demo.hello",
      payload: Schema.Struct({
        message: Schema.String
      })
    })

    const jobLayer = job.toLayer((payload) =>
      Effect.succeed({ ok: payload.message.length > 0 })
    )

    const program = Effect.gen(function* () {
      const registry = yield* JobRegistry
      const handler = yield* registry.get("demo.hello")
      const handlers = yield* registry.list

      return { handler, handlers }
    }).pipe(Effect.provide(jobLayer.pipe(Layer.provide(JobRegistryMemory))))

    const result = await Effect.runPromise(program)

    expect(Option.isSome(result.handler)).toBe(true)
    expect(result.handlers).toHaveLength(1)
    expect(result.handlers[0]?.job.name).toBe("demo.hello")
  })

  it("fails fast on duplicate handler registration", async () => {
    const job = Job.make({
      name: "demo.duplicate",
      payload: Schema.Struct({
        message: Schema.String
      })
    })

    const first = job.toLayer(() => Effect.void)
    const second = job.toLayer(() => Effect.void)

    const program = Effect.gen(function* () {
      const registry = yield* JobRegistry
      return yield* registry.list
    }).pipe(
      Effect.provide(Layer.mergeAll(first, second).pipe(Layer.provide(JobRegistryMemory)))
    )

    await expect(Effect.runPromise(program)).rejects.toMatchObject({
      _tag: "DuplicateJobHandlerError",
      jobName: "demo.duplicate"
    })
  })
})
