import type { ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import { forkParked } from "../serverActivation.ts";
import * as WorktreeProtection from "../workspace/WorktreeProtection.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectStore from "./ProjectStore.ts";

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projects = yield* ProjectStore.ProjectStoreV2;
    const protection = yield* WorktreeProtection.WorktreeProtection;
    const reconcile = (projectId: ProjectId) =>
      // A deleted project's threads no longer use its worktrees, so its locks must go.
      projects.get(projectId, { includeDeleted: true }).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.void,
            onSome: (project) => protection.reconcile(project.workspaceRoot),
          }),
        ),
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to reconcile worktree locks", { projectId, cause }),
        ),
      );
    yield* forkParked(
      Stream.runForEach(orchestrator.streamDomainEvents, (event) =>
        event.type === "thread.created" ||
        event.type === "thread.archived" ||
        event.type === "thread.unarchived" ||
        event.type === "thread.deleted" ||
        event.type === "thread.metadata-updated"
          ? reconcile(event.payload.projectId)
          : Effect.void,
      ),
    );
    // Covers threads from before T3 took locks, and changes made while the server was down.
    yield* forkParked(
      projects.listShells().pipe(
        Effect.flatMap((shells) =>
          Effect.forEach(shells, (project) => reconcile(project.id), { discard: true }),
        ),
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to reconcile worktree locks at startup", { cause }),
        ),
      ),
    );
  }),
);
