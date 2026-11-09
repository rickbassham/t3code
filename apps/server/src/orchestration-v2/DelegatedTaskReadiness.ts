import { type NodeId, ThreadId } from "@t3tools/contracts";
import { delegatedTaskProgress } from "@t3tools/provider-core/server/subagentProjection";
import { runRanAfter } from "@t3tools/shared/orchestrationV2ThreadError";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as EffectOutbox from "./EffectOutbox.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

export class DelegatedTaskReadinessError extends Schema.TaggedError<DelegatedTaskReadinessError>()(
  "DelegatedTaskReadinessError",
  { childThreadId: ThreadId, cause: Schema.Defect() },
) {}

export class DelegatedTaskReadiness extends Context.Service<
  DelegatedTaskReadiness,
  {
    readonly pending: (
      childThreadId: ThreadId,
      taskId?: NodeId,
    ) => Effect.Effect<boolean, DelegatedTaskReadinessError>;
  }
>()("t3/orchestration-v2/DelegatedTaskReadiness") {}

const make = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  return DelegatedTaskReadiness.of({
    pending: Effect.fn("DelegatedTaskReadiness.pending")(
      function* (childThreadId, taskId) {
        const child = yield* projections.getThreadRecords(
          childThreadId,
          ["runs", "messages", "subagents", "providerThreads", "providerTurns", "attempts"],
          { messageRoles: ["user"] },
        );
        const taskRuns = child.runs.filter((run) => run.delegatedTaskId === taskId);
        const legacyOriginal =
          child.thread.forkedFrom?.type === "node" &&
          child.thread.forkedFrom.nodeId === taskId &&
          child.runs.every((run) => run.delegatedTaskId === undefined);
        const legacy = taskId === undefined || legacyOriginal;
        const progress = legacy ? delegatedTaskProgress(child) : undefined;
        if (legacy && progress?.state !== "result_available") return true;
        const runs = legacy ? child.runs : taskRuns;
        const result = legacy
          ? progress?.resultRun
          : runs.toSorted((a, b) => b.ordinal - a.ordinal)[0];
        if (result === undefined) return !legacy;
        if (
          !["completed", "failed", "cancelled", "interrupted", "rolled_back"].includes(
            result.status,
          )
        )
          return true;
        const pendingRestarts = yield* Effect.forEach(
          runs.filter((run) => run.id === result.id || runRanAfter(run, result)),
          (run) =>
            outbox
              .get(`effect:restart-continuation:${run.id}`)
              .pipe(
                Effect.map(
                  Option.exists(
                    (effect) => effect.status === "pending" || effect.status === "running",
                  ),
                ),
              ),
        );
        return pendingRestarts.includes(true);
      },
      (effect, childThreadId, _taskId) =>
        effect.pipe(
          Effect.mapError((cause) => new DelegatedTaskReadinessError({ childThreadId, cause })),
        ),
    ),
  });
});

export const layer = Layer.effect(DelegatedTaskReadiness, make);
