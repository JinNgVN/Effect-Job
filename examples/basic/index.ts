import { Duration, Effect, Schema } from "effect"
import { Job } from "../../src"

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

const HelloJobLive = HelloJob.toLayer((payload, ctx) =>
  Effect.gen(function* () {
    yield* ctx.progress({ stage: "running" })
    yield* Effect.logInfo(payload.message)
    return { ok: true }
  })
)

Effect.runSync(Effect.logInfo(`Defined job: ${HelloJob.name}; layer created: ${String(Boolean(HelloJobLive))}`))
