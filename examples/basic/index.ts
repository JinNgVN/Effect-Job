import { Duration, Effect, Layer, Option, Schema } from "effect"
import { JobEngine, JobEngineMemory } from "../../src/engine"
import { Job } from "../../src/jobImpl"
import { JobRegistry, JobRegistryMemory } from "../../src/registry"

const HelloJob = Job.make({
  name: "demo.hello",
  queue: "demo",
  payload: Schema.Struct({
    message: Schema.String
  }),
  success: Schema.Struct({
    ok: Schema.Boolean
  }),
  attempts: 3,
  timeout: Duration.seconds(30)
})

const HelloJobLive = HelloJob.toLayer((payload) =>
  Effect.gen(function* () {
    yield* Effect.logInfo(payload.message)
    return { ok: true }
  })
)

const program = Effect.gen(function* () {
  const registry = yield* JobRegistry
  const handler = yield* registry.get(HelloJob.name)
  const handlers = yield* registry.list
  const handle = yield* Job.enqueue(HelloJob, { message: "hello" })
  const bulkHandles = yield* Job.enqueueMany(HelloJob, [
    { message: "bulk one" },
    { message: "bulk two" }
  ])
  const engine = yield* JobEngine
  const record = yield* engine.find(handle.id)
  const records = yield* engine.list

  yield* Effect.logInfo(`Defined job: ${HelloJob.name}`)
  yield* Effect.logInfo(`Handler registered: ${String(Option.isSome(handler))}`)
  yield* Effect.logInfo(`Registered handler count: ${handlers.length}`)
  yield* Effect.logInfo(`Enqueued job id: ${handle.id}`)
  yield* Effect.logInfo(`Bulk enqueued count: ${bulkHandles.length}`)
  yield* Effect.logInfo(`Stored job found: ${String(Option.isSome(record))}`)
  yield* Effect.logInfo(`Stored job count: ${records.length}`)
})

Effect.runSync(
  program.pipe(
    Effect.provide(
      Layer.mergeAll(
        HelloJobLive.pipe(Layer.provide(JobRegistryMemory)),
        JobEngineMemory
      )
    )
  )
)
