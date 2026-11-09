import type {
  OrchestrationV2AppThread,
  OrchestrationV2ProviderGoal,
  OrchestrationV2ProviderThread,
} from "@t3tools/contracts";

import { threadPullRequestKeysEqual, threadPullRequestsOf } from "./threadPullRequests.ts";

/** The app goal stays active while T3 suspends native continuation for PR news. */
export function presentProviderThreadGoal(
  providerThread:
    | Pick<OrchestrationV2ProviderThread, "goal" | "goalWaitForPullRequest">
    | undefined,
  thread: Pick<OrchestrationV2AppThread, "pullRequests" | "linkedPullRequest">,
): OrchestrationV2ProviderGoal | null {
  const goal = providerThread?.goal ?? null;
  const wait = providerThread?.goalWaitForPullRequest;
  if (goal?.status !== "paused" || wait == null || wait.resumeOnUpdate === false) return goal;
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
