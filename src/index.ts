import { Effect, Layer, Schema } from "effect";

import { JobEngine, JobEngineMemory } from "./engine";
import { Job } from "./jobImpl";
import { JobRegistry, JobRegistryMemory } from "./registry";
import { Worker } from "./worker";

export * from "./engine";
export * from "./enqueue";
export * from "./job";
export * from "./jobImpl";
export * from "./registry";
export * from "./worker";

const SendEmail = Job.make({
    name: "email.send",
    queue: "mailers",
    payload: Schema.Struct({
        to: Schema.String,
        subject: Schema.String,
        body: Schema.String,
    }),
    success: Schema.Struct({
        sentAt: Schema.Date,
    }),
    idempotencyKey: (payload) => `${payload.to}:${payload.subject}`,
});

const SendEmailLive = SendEmail.toLayer((payload) =>
    Effect.gen(function* () {
        yield* Effect.logInfo(`Sending email to ${payload.to}`);

        return {
            sentAt: new Date(),
        };
    }),
);

const GenerateInvoice = Job.make({
    name: "invoice.generate",
    queue: "billing",
    payload: Schema.Struct({
        customerId: Schema.String,
        month: Schema.String,
    }),
    success: Schema.Struct({
        invoiceId: Schema.String,
    }),
    idempotencyKey: (payload) => `${payload.customerId}:${payload.month}`,
});

const GenerateInvoiceLive = GenerateInvoice.toLayer((payload) =>
    Effect.gen(function* () {
        yield* Effect.logInfo(
            `Generating ${payload.month} invoice for ${payload.customerId}`,
        );

        return {
            invoiceId: "invoice_123",
        };
    }),
);

const JobsLive = Layer.mergeAll(SendEmailLive, GenerateInvoiceLive).pipe(
    Layer.provide(JobRegistryMemory),
);

const program = Effect.gen(function* () {
    const email = yield* Job.enqueue(SendEmail, {
        to: "ada@example.com",
        subject: "Welcome",
        body: "Thanks for signing up.",
    });

    const invoice = yield* Job.enqueue(GenerateInvoice, {
        customerId: "customer_123",
        month: "2026-05",
    });

    const registry = yield* JobRegistry;
    const registeredJobs = yield* registry.list;

    const engine = yield* JobEngine;
    const queued = yield* engine.list;

    yield* Worker.runOnce({ queue: "mailers" });
    yield* Worker.runOnce({ queue: "billing" });

    const completed = yield* engine.list;

    yield* Effect.logInfo(`Queued ${email.name} as ${email.id}`);
    yield* Effect.logInfo(`Queued ${invoice.name} as ${invoice.id}`);
    yield* Effect.logInfo(`Registered handlers: ${registeredJobs.length}`);
    yield* Effect.logInfo(`Stored jobs before worker: ${queued.length}`);
    yield* Effect.logInfo(
        `Completed jobs: ${completed.filter((job) => job.status === "completed").length}`,
    );
});

const AppLive = Layer.mergeAll(JobsLive, JobEngineMemory);

const runnable = program.pipe(Effect.provide(AppLive));

Effect.runSync(runnable);
