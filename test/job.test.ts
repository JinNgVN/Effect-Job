import { Schema } from "effect"
import { describe, expect, it } from "vitest"
import { Job } from "../src"

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
    expect(job.attempts).toBe(3)
    expect(typeof job.toLayer).toBe("function")
  })
})
