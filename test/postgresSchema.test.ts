import { describe, expect, it } from "vitest";

import { postgresMigration } from "../src";

describe("postgresMigration", () => {
    it("creates the default jobs table and worker indexes", () => {
        const migration = postgresMigration();

        expect(migration.up).toContain(
            'CREATE TABLE IF NOT EXISTS "public"."effect_jobs"',
        );
        expect(migration.up).toContain("payload jsonb NOT NULL");
        expect(migration.up).toContain("meta jsonb NOT NULL");
        expect(migration.up).toContain("tags text[] NOT NULL");
        expect(migration.up).toContain("attempted_by text[] NOT NULL");
        expect(migration.up).toContain(
            "WHERE status IN ('available', 'scheduled', 'retryable')",
        );
        expect(migration.up).toContain(
            "WHERE idempotency_key IS NOT NULL AND status IN ('suspended', 'available', 'scheduled', 'executing', 'retryable')",
        );
        expect(migration.down).toBe('DROP TABLE IF EXISTS "public"."effect_jobs";');
    });

    it("quotes custom schema and table names", () => {
        const migration = postgresMigration({
            schema: 'tenant"one',
            table: 'jobs"queue',
        });

        expect(migration.up).toContain(
            'CREATE SCHEMA IF NOT EXISTS "tenant""one";',
        );
        expect(migration.up).toContain(
            'CREATE TABLE IF NOT EXISTS "tenant""one"."jobs""queue"',
        );
        expect(migration.down).toBe(
            'DROP TABLE IF EXISTS "tenant""one"."jobs""queue";',
        );
    });
});
