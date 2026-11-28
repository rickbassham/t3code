import { assert, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";

import * as ServerConfig from "../config.ts";
import * as GitManager from "../git/GitManager.ts";
import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as VcsProjectConfig from "../vcs/VcsProjectConfig.ts";
import * as WorktreeProtection from "./WorktreeProtection.ts";

const layerNode = Layer.mergeAll(NodeServices.layer, NodeCrypto.layer);
const owner = ThreadId.make("thread-owner");
const other = ThreadId.make("thread-other");

const setup = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const baseDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-worktree-protection-" });
  const workspaceRoot = path.join(baseDir, "project");
  // ServerConfig.layerTest puts the managed worktrees folder here.
  const worktree = path.join(baseDir, "worktrees", "project", "thread");
  let users: ReadonlyArray<ThreadId> = [];
  const context = yield* Layer.build(
    GitWorkflowService.layer.pipe(
      Layer.provide(Layer.mock(GitManager.GitManager)({})),
      Layer.provide(VcsDriverRegistry.layer.pipe(Layer.provide(VcsProjectConfig.layer))),
      Layer.provideMerge(WorktreeProtection.layer),
      Layer.provide(
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadWorktreePaths: () =>
            Effect.sync(() => users.map((threadId) => ({ threadId, worktreePath: worktree }))),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provideMerge(GitVcsDriver.layer),
      Layer.provideMerge(VcsProcess.layer),
      Layer.provideMerge(
        ServerConfig.layerTest(process.cwd(), baseDir).pipe(Layer.provide(layerNode)),
      ),
    ),
  );
  const driver = Context.get(context, GitVcsDriver.GitVcsDriver);
  const git = (args: ReadonlyArray<string>) =>
    driver.execute({
      operation: "test.worktreeProtection",
      cwd: workspaceRoot,
      args,
      allowNonZeroExit: true,
    });
  yield* fileSystem.makeDirectory(workspaceRoot);
  for (const args of [
    ["init", "-b", "main"],
    ["-c", "user.email=t@t", "-c", "user.name=t", "commit", "--allow-empty", "-m", "init"],
    ["worktree", "add", "-b", "thread", worktree],
  ]) {
    yield* git(args);
  }
  return {
    workspaceRoot,
    worktree,
    git,
    exists: fileSystem.exists(worktree),
    deleteFolder: fileSystem.remove(worktree, { recursive: true }),
    makeDirty: fileSystem.writeFileString(path.join(worktree, "untracked.txt"), "work"),
    setUsers: (next: ReadonlyArray<ThreadId>) => {
      users = next;
    },
    protection: Context.get(context, WorktreeProtection.WorktreeProtection),
    workflow: Context.get(context, GitWorkflowService.GitWorkflowService),
  };
});

it.live("locks a managed worktree while an unarchived thread uses it", () =>
  Effect.gen(function* () {
    const repo = yield* setup;
    repo.setUsers([owner]);
    yield* repo.protection.reconcile(repo.workspaceRoot);
    yield* repo.git(["worktree", "remove", "--force", repo.worktree]);
    assert.isTrue(yield* repo.exists);

    repo.setUsers([]);
    yield* repo.protection.reconcile(repo.workspaceRoot);
    yield* repo.git(["worktree", "remove", "--force", repo.worktree]);
    assert.isFalse(yield* repo.exists);
  }).pipe(Effect.scoped, Effect.provide(layerNode)),
);

it.live("keeps a lock that T3 did not take", () =>
  Effect.gen(function* () {
    const repo = yield* setup;
    yield* repo.git(["worktree", "lock", "--reason", "mine", repo.worktree]);
    yield* repo.protection.reconcile(repo.workspaceRoot);
    yield* repo.git(["worktree", "remove", "--force", repo.worktree]);
    assert.isTrue(yield* repo.exists);
  }).pipe(Effect.scoped, Effect.provide(layerNode)),
);

it.live("removes a locked worktree only when no other unarchived thread uses it", () =>
  Effect.gen(function* () {
    const repo = yield* setup;
    const input = { cwd: repo.workspaceRoot, path: repo.worktree, force: true };
    repo.setUsers([owner, other]);
    yield* repo.protection.reconcile(repo.workspaceRoot);
    const refused = yield* Effect.exit(
      repo.workflow.removeWorktree(input, { exceptThreadId: owner }),
    );
    assert.isTrue(Exit.isFailure(refused));
    assert.isTrue(yield* repo.exists);

    repo.setUsers([owner]);
    assert.isTrue(Exit.isFailure(yield* Effect.exit(repo.workflow.removeWorktree(input))));
    assert.isTrue(yield* repo.exists);
    yield* repo.workflow.removeWorktree(input, { exceptThreadId: owner });
    assert.isFalse(yield* repo.exists);
  }).pipe(Effect.scoped, Effect.provide(layerNode)),
);

it.live("locks the worktree again when its removal fails", () =>
  Effect.gen(function* () {
    const repo = yield* setup;
    yield* repo.makeDirty;
    repo.setUsers([owner]);
    yield* repo.protection.reconcile(repo.workspaceRoot);
    const failed = yield* Effect.exit(
      repo.workflow.removeWorktree(
        { cwd: repo.workspaceRoot, path: repo.worktree },
        { exceptThreadId: owner },
      ),
    );
    assert.isTrue(Exit.isFailure(failed));
    yield* repo.git(["worktree", "remove", "--force", repo.worktree]);
    assert.isTrue(yield* repo.exists);
  }).pipe(Effect.scoped, Effect.provide(layerNode)),
);

it.live("lets a worktree be recreated after its folder was deleted", () =>
  Effect.gen(function* () {
    const repo = yield* setup;
    repo.setUsers([owner]);
    yield* repo.protection.reconcile(repo.workspaceRoot);
    yield* repo.deleteFolder;
    yield* repo.workflow.pruneWorktrees({ cwd: repo.workspaceRoot });
    const added = yield* repo.git(["worktree", "add", repo.worktree, "thread"]);
    assert.strictEqual(added.exitCode, 0, added.stderr);
  }).pipe(Effect.scoped, Effect.provide(layerNode)),
);
