import { assert, it } from "@effect/vitest";
import { GitCommandError, type OrchestrationV2DomainEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import * as WorktreeProtection from "../workspace/WorktreeProtection.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as WorktreeProtectionSync from "./WorktreeProtectionSync.ts";

it.effect("reconciles every project at startup, then a project when its threads change", () =>
  Effect.gen(function* () {
    const events = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
    const reconciled = yield* Queue.unbounded<string>();
    yield* Layer.build(
      WorktreeProtectionSync.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(Orchestrator.OrchestratorV2)({
              streamDomainEvents: Stream.fromQueue(events),
            }),
            Layer.mock(ProjectStore.ProjectStoreV2)({
              listShells: () => Effect.succeed([{ id: "first" }, { id: "second" }] as never),
              get: (projectId) => Effect.succeedSome({ workspaceRoot: `/${projectId}` } as never),
            }),
            Layer.mock(WorktreeProtection.WorktreeProtection)({
              reconcile: (cwd) =>
                Queue.offer(reconciled, cwd).pipe(
                  Effect.andThen(
                    cwd === "/failing"
                      ? Effect.fail(
                          new GitCommandError({
                            operation: "test.reconcile",
                            command: "git worktree lock",
                            cwd,
                            detail: "failed",
                          }),
                        )
                      : Effect.void,
                  ),
                ),
            }),
          ),
        ),
      ),
    );
    assert.deepEqual(yield* Queue.takeN(reconciled, 2), ["/first", "/second"]);

    yield* Queue.offerAll(events, [
      { type: "run.updated", payload: { projectId: "ignored" } },
      { type: "thread.archived", payload: { projectId: "failing" } },
      { type: "thread.metadata-updated", payload: { projectId: "moved" } },
    ] as never);
    assert.deepEqual(yield* Queue.takeN(reconciled, 2), ["/failing", "/moved"]);
  }).pipe(Effect.scoped),
);
