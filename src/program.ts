import { Console, Effect, Schedule } from "effect";

const job = Effect.repeat(
    Console.log("child: still running"),
    Schedule.fixed("1 second"),
);

const parent = Effect.gen(function* () {
    yield* Console.log("parent: started");

    yield* Effect.forkScoped(job);

    yield* Effect.sleep("2 seconds");
    yield* Console.log("parent: finished");
});

const program = Effect.scoped(
    Effect.gen(function* () {
        yield* Console.log("scope: opened");

        yield* Effect.forkChild(parent);

        yield* Effect.sleep("5 seconds");

        yield* Console.log("scope: closing");
    }),
);

Effect.runPromise(program);
