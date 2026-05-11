import { Effect } from "effect";

import { Job, JobSystem } from "../../src";
import { Catalog, EchoJob } from "./jobs";

const jobs = JobSystem.memory({
    catalog: Catalog,
    jobs: [EchoJob],
    queues: {
        dev: { concurrency: 1, pollInterval: "250 millis" },
    },
});

const json = (body: unknown, init?: ResponseInit) =>
    new Response(JSON.stringify(body, null, 2), {
        ...init,
        headers: {
            "content-type": "application/json",
            ...init?.headers,
        },
    });

const insertEcho = async (request: Request) => {
    const body = await request.json();
    const message =
        typeof body === "object" &&
        body !== null &&
        "message" in body &&
        typeof body.message === "string"
            ? body.message
            : undefined;

    if (message === undefined) {
        return json(
            { error: "Expected JSON body: { \"message\": string }" },
            { status: 400 },
        );
    }

    const handle = await jobs.runPromise(EchoJob.enqueue({ message }));

    return json({ job: handle });
};

const listJobs = () => jobs.runPromise(Job.list());

void jobs.runPromise(jobs.worker());

const server = Bun.serve({
    port: 3000,
    async fetch(request: Request) {
        const url = new URL(request.url);

        if (request.method === "GET" && url.pathname === "/health") {
            return json({ ok: true });
        }

        if (request.method === "GET" && url.pathname === "/jobs") {
            return json({ jobs: await listJobs() });
        }

        if (request.method === "POST" && url.pathname === "/jobs/echo") {
            return insertEcho(request);
        }

        return json(
            {
                error: "Not found",
                routes: ["GET /health", "GET /jobs", "POST /jobs/echo"],
            },
            { status: 404 },
        );
    },
});

console.log(`effect-job dev server listening on ${server.url}`);
console.log("send a job: bun run dev:send \"hello from another terminal\"");

const shutdown = async () => {
    server.stop();
    await jobs.dispose();
    process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
