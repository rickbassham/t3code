import * as Schema from "effect/Schema";
import { OrchestrationV2GoalWait as GoalWaitSchema } from "@t3tools/contracts";
import type {
  OrchestrationV2AppThread,
  OrchestrationV2ProviderGoal,
  OrchestrationV2GoalWait,
  OrchestrationV2ProviderThread,
} from "@t3tools/contracts";

import { threadPullRequestKeysEqual, threadPullRequestsOf } from "./threadPullRequests.ts";

export const goalWaitEqual = Schema.toEquivalence(GoalWaitSchema);

export function providerGoalWait(
  thread: Pick<OrchestrationV2ProviderThread, "goalWait" | "goalWaitForPullRequest"> | undefined,
): OrchestrationV2GoalWait | null | undefined {
  return thread?.goalWait !== undefined ? thread.goalWait : thread?.goalWaitForPullRequest;
}

export function normalizeProviderGoalWait(thread: OrchestrationV2ProviderThread) {
  const { goalWaitForPullRequest, ...current } = thread;
  return thread.goalWait === undefined && goalWaitForPullRequest !== undefined
    ? { ...current, goalWait: goalWaitForPullRequest }
    : current;
}

/** The app goal stays active while T3 suspends native continuation for external results. */
export function presentProviderThreadGoal(
  providerThread:
    | Pick<OrchestrationV2ProviderThread, "goal" | "goalWait" | "goalWaitForPullRequest">
    | undefined,
  thread: Pick<OrchestrationV2AppThread, "pullRequests" | "linkedPullRequest">,
): OrchestrationV2ProviderGoal | null {
  const goal = providerThread?.goal ?? null;
  const wait = providerGoalWait(providerThread);
  if (goal?.status !== "paused" || wait == null || wait.resumeOnUpdate === false) return goal;
  if (wait.kind === "tasks") return { ...goal, status: "active", waitingForTasks: wait.taskIds };
  const watching = threadPullRequestsOf(thread).some(
    (link) => link.watch?.startedAt === wait.startedAt && threadPullRequestKeysEqual(link, wait),
  );
  if (!watching) return goal;
  return {
    ...goal,
    status: "active",
    waitingForPullRequest: { host: wait.host, repository: wait.repository, number: wait.number },
  };
}
