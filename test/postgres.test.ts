import { describe, expect, it } from "vitest";

import { postgresRowToJobRecord } from "../src";

describe("postgresRowToJobRecord", () => {
    it("maps the persisted postgres shape into the public job record shape", () => {
        const record = postgresRowToJobRecord({
            id: "job-1",
            name: "email.send",
            queue: "mailers",
            payload: { to: "person@example.com" },
            meta: { source: "signup" },
            tags: ["welcome"],
            status: "available",
            priority: 1,
            attempt: 0,
            executions: 0,
            snoozes: 0,
            max_attempts: 20,
            run_at: "2026-05-09T01:00:00.000Z",
            idempotency_key: "welcome:person@example.com",
            attempted_at: null,
            attempted_by: [],
            completed_at: null,
            cancelled_at: null,
            discarded_at: null,
            errors: [],
            inserted_at: "2026-05-09T00:59:00.000Z",
            updated_at: "2026-05-09T00:59:00.000Z",
        });

        expect(record).toMatchObject({
            id: "job-1",
            name: "email.send",
            queue: "mailers",
            meta: { source: "signup" },
            tags: ["welcome"],
            status: "available",
            attempt: 0,
            executions: 0,
            snoozes: 0,
            maxAttempts: 20,
            idempotencyKey: "welcome:person@example.com",
        });
        expect(record.runAt.toISOString()).toBe("2026-05-09T01:00:00.000Z");
        expect(record.insertedAt.toISOString()).toBe(
            "2026-05-09T00:59:00.000Z",
        );
    });
});
