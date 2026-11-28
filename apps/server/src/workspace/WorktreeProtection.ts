import { GitCommandError, type ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Semaphore from "effect/Semaphore";

import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import { makeIsManagedWorktreePath } from "../worktreesDirectory.ts";

const LOCK_REASON = "T3 Code: resumable thread workspace";

export class WorktreeProtection extends Context.Service<
  WorktreeProtection,
  {
    /** Locks the managed worktrees that unarchived threads use, and releases T3's other locks. */
    readonly reconcile: (cwd: string) => Effect.Effect<void, GitCommandError>;
    /** Fails when an unarchived thread other than `exceptThreadId` uses the worktree. */
    readonly assertRemovable: (input: {
      readonly cwd: string;
      readonly path: string;
      readonly exceptThreadId?: ThreadId | undefined;
    }) => Effect.Effect<void, GitCommandError>;
    /** Runs `effect` with T3's lock released, then reconciles. Locks T3 did not take stay. */
    readonly withoutLock: <A, E, R>(
      input: { readonly cwd: string; readonly path: string },
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | GitCommandError, R>;
  }
>()("t3/workspace/WorktreeProtection") {}

const make = Effect.gen(function* () {
  const git = yield* GitVcsDriver.GitVcsDriver;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const isManagedWorktreePath = yield* makeIsManagedWorktreePath;
  // One reconcile at a time: Git fails a second unlock of the same worktree.
  const mutex = Semaphore.makeUnsafe(1);
  const released = new Set<string>();

  // Git lists symlink-resolved paths; threads store the path they were given.
  const realPath = (target: string) =>
    fileSystem.realPath(target).pipe(Effect.orElseSucceed(() => target));

  const usedPaths = Effect.fn("WorktreeProtection.usedPaths")(function* (
    cwd: string,
    exceptThreadId?: ThreadId,
  ) {
    const threads = yield* projections.getThreadWorktreePaths().pipe(
      Effect.mapError(
        (cause) =>
          new GitCommandError({
            operation: "WorktreeProtection.usedPaths",
            command: "git worktree lock",
            cwd,
            detail: "Could not read which threads use this project's worktrees.",
            cause,
          }),
      ),
    );
    const paths = new Set(
      threads
        .filter((thread) => thread.threadId !== exceptThreadId)
        .map((thread) => path.resolve(thread.worktreePath)),
    );
    return new Set(yield* Effect.forEach(paths, realPath));
  });

  const listLinkedWorktrees = Effect.fn("WorktreeProtection.listLinkedWorktrees")(function* (
    cwd: string,
  ) {
    const input = {
      operation: "WorktreeProtection.listLinkedWorktrees",
      cwd,
      args: ["worktree", "list", "--porcelain"],
      env: { LC_ALL: "C" },
    };
    let result = yield* git.execute({
      ...input,
      args: [...input.args, "-z"],
      allowNonZeroExit: true,
    });
    if (result.exitCode !== 0) {
      if (result.stderr.toLowerCase().includes("not a git repository")) return [];
      // Pre-2.36 Git rejects worktree list -z; normalize its line fallback to NUL records.
      const lines = yield* git.execute(input);
      result = { ...lines, stdout: lines.stdout.replaceAll("\n", "\0") };
    }
    // Git lists the main worktree first, and it cannot be locked.
    return result.stdout
      .split("\0\0")
      .slice(1)
      .flatMap((record) => {
        const fields = record.split("\0");
        const worktree = fields.find((field) => field.startsWith("worktree "));
        return worktree === undefined
          ? []
          : [
              {
                path: path.resolve(cwd, worktree.slice("worktree ".length)),
                lock: fields.find((field) => field === "locked" || field.startsWith("locked ")),
              },
            ];
      });
  });

  const reconcile: WorktreeProtection["Service"]["reconcile"] = Effect.fn(
    "WorktreeProtection.reconcile",
  )(function* (cwd) {
    yield* mutex.withPermit(
      Effect.gen(function* () {
        const worktrees = yield* listLinkedWorktrees(cwd);
        if (worktrees.length === 0) return;
        const used = yield* usedPaths(cwd);
        for (const worktree of worktrees) {
          const location = yield* realPath(worktree.path);
          // Turn start recreates a missing folder, so a lock on one only blocks that.
          const wanted =
            used.has(location) &&
            !released.has(location) &&
            (yield* fileSystem.exists(worktree.path).pipe(Effect.orElseSucceed(() => false))) &&
            (yield* isManagedWorktreePath(worktree.path));
          if (wanted && worktree.lock === undefined) {
            yield* git.execute({
              operation: "WorktreeProtection.lock",
              cwd,
              args: ["worktree", "lock", "--reason", LOCK_REASON, "--", worktree.path],
            });
          } else if (!wanted && worktree.lock === `locked ${LOCK_REASON}`) {
            yield* git.execute({
              operation: "WorktreeProtection.unlock",
              cwd,
              args: ["worktree", "unlock", "--", worktree.path],
            });
          }
        }
      }),
    );
  });

  const assertRemovable: WorktreeProtection["Service"]["assertRemovable"] = Effect.fn(
    "WorktreeProtection.assertRemovable",
  )(function* (input) {
    const used = yield* usedPaths(input.cwd, input.exceptThreadId);
    if (!used.has(yield* realPath(path.resolve(input.cwd, input.path)))) return;
    return yield* new GitCommandError({
      operation: "WorktreeProtection.assertRemovable",
      command: "git worktree remove",
      cwd: input.cwd,
      detail:
        "Another thread still uses this worktree. Archive or delete that thread, or move it to another worktree, then try again.",
    });
  });

  const withoutLock: WorktreeProtection["Service"]["withoutLock"] = (input, effect) =>
    Effect.acquireUseRelease(
      realPath(path.resolve(input.cwd, input.path)).pipe(
        Effect.tap((location) => Effect.sync(() => released.add(location))),
      ),
      () => reconcile(input.cwd).pipe(Effect.andThen(effect)),
      (location) =>
        Effect.suspend(() => {
          released.delete(location);
          return reconcile(input.cwd);
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Failed to restore the worktree lock", { ...input, cause }),
          ),
        ),
    );

  return WorktreeProtection.of({ reconcile, assertRemovable, withoutLock });
});

export const layer = Layer.effect(WorktreeProtection, make);
