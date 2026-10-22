import { GitCommandError, type ThreadId, type ProjectId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import { makeIsManagedWorktreePath } from "../worktreesDirectory.ts";
import { withWorkspaceLease } from "./workspaceLease.ts";
import { WORKTREE_PROTECTION_REASON } from "./worktreeProtectionReason.ts";

interface WorktreeLocation {
  readonly cwd: string;
  readonly path: string;
}
export class WorktreeProtection extends Context.Service<
  WorktreeProtection,
  {
    /** Caller holds withLease across reference mutation or removal. */
    readonly protect: (input: WorktreeLocation) => Effect.Effect<void, GitCommandError>;
    readonly assertRemovable: (input: WorktreeLocation) => Effect.Effect<void, GitCommandError>;
    readonly reconcile: (input: WorktreeLocation) => Effect.Effect<void, GitCommandError>;
    readonly reconcileThread: (threadId: ThreadId) => Effect.Effect<void, GitCommandError>;
    readonly reconcileAll: Effect.Effect<void, GitCommandError>;
    readonly withReference: <A, E, R>(
      input: { readonly projectId: ProjectId; readonly worktreePath: string },
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | GitCommandError, R>;
    readonly withLease: <A, E, R>(
      input: WorktreeLocation,
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | GitCommandError, R>;
  }
>()("t3/workspace/WorktreeProtection") {}

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const git = yield* GitVcsDriver.GitVcsDriver;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const managed = yield* makeIsManagedWorktreePath;
  const previous = new Map<ThreadId, WorktreeLocation>();
  const knownPaths = new Map<string, WorktreeLocation>();
  const failure = (input: WorktreeLocation, detail: string, cause?: unknown) =>
    new GitCommandError({
      operation: "WorktreeProtection",
      command: "git worktree lock",
      cwd: input.cwd,
      detail,
      ...(cause === undefined ? {} : { cause }),
    });
  const canonical = (target: string): Effect.Effect<string, GitCommandError> =>
    fs.realPath(target).pipe(
      Effect.catch((error) => {
        if (error.reason._tag !== "NotFound")
          return Effect.fail(
            failure({ cwd: target, path: target }, "Could not resolve worktree location.", error),
          );
        const parent = path.dirname(target);
        return parent === target
          ? Effect.succeed(target)
          : canonical(parent).pipe(Effect.map((real) => path.join(real, path.basename(target))));
      }),
    );
  const withLease: WorktreeProtection["Service"]["withLease"] = (input, effect) =>
    canonical(path.resolve(input.cwd, input.path)).pipe(
      Effect.flatMap((target) => withWorkspaceLease(target, effect)),
    );
  const list = Effect.fn("WorktreeProtection.list")(function* (cwd: string) {
    const result = yield* git.execute({
      operation: "WorktreeProtection.list",
      cwd,
      args: ["worktree", "list", "--porcelain", "-z"],
    });
    const listed = result.stdout.split("\0\0").flatMap((record) => {
      const fields = record.split("\0");
      const location = fields.find((field) => field.startsWith("worktree "));
      if (location === undefined) return [];
      return [
        {
          path: location.slice(9),
          locked: fields.find((field) => field === "locked" || field.startsWith("locked ")),
          bare: fields.includes("bare"),
        },
      ];
    });
    return yield* Effect.forEach(listed, (entry) =>
      canonical(path.resolve(entry.path)).pipe(Effect.map((real) => ({ ...entry, path: real }))),
    );
  });
  const protect: WorktreeProtection["Service"]["protect"] = Effect.fn("WorktreeProtection.protect")(
    function* (input) {
      const target = yield* canonical(path.resolve(input.cwd, input.path));
      if (!(yield* managed(target))) return;
      const worktree = (yield* list(input.cwd)).find(
        (entry) => path.resolve(entry.path) === target,
      );
      if (worktree === undefined || worktree.bare || worktree.locked !== undefined) return;
      yield* git.execute({
        operation: "WorktreeProtection.protect",
        cwd: input.cwd,
        args: ["worktree", "lock", "--reason", WORKTREE_PROTECTION_REASON, "--", target],
      });
    },
  );
  const references = Effect.fn("WorktreeProtection.references")(function* (
    input: WorktreeLocation,
  ) {
    const target = yield* canonical(path.resolve(input.cwd, input.path));
    const rows = yield* projections
      .getThreadWorktreePaths({ includeBusyRetired: true })
      .pipe(
        Effect.mapError((cause) =>
          failure(input, "Could not verify worktree thread references.", cause),
        ),
      );
    return yield* Effect.filter(rows, (row) =>
      canonical(path.resolve(row.worktreePath)).pipe(
        Effect.map((candidate) => candidate === target),
      ),
    );
  });
  const assertRemovable: WorktreeProtection["Service"]["assertRemovable"] = Effect.fn(
    "WorktreeProtection.assertRemovable",
  )(function* (input) {
    if ((yield* references(input)).length > 0)
      return yield* failure(
        input,
        "This worktree is still referenced by a resumable T3 thread. Archive, delete, or move every referencing thread before removing it.",
      );
  });
  const reconcile: WorktreeProtection["Service"]["reconcile"] = Effect.fn(
    "WorktreeProtection.reconcile",
  )(function* (input) {
    if ((yield* references(input)).length > 0) return yield* protect(input);
    const target = yield* canonical(path.resolve(input.cwd, input.path));
    if (!(yield* managed(target))) return;
    const worktree = (yield* list(input.cwd)).find((entry) => path.resolve(entry.path) === target);
    if (worktree?.locked !== `locked ${WORKTREE_PROTECTION_REASON}`) return;
    yield* git.execute({
      operation: "WorktreeProtection.release",
      cwd: input.cwd,
      args: ["worktree", "unlock", "--", target],
    });
  });
  const reconcileAll = Effect.gen(function* () {
    const roots = yield* projects
      .listShells()
      .pipe(
        Effect.mapError((cause) =>
          failure({ cwd: ".", path: "." }, "Could not read project workspaces.", cause),
        ),
      );
    const rows = yield* projections
      .getThreadWorktreePaths({
        includeArchived: true,
        includeDeleted: true,
        includeBusyRetired: true,
      })
      .pipe(
        Effect.mapError((cause) =>
          failure({ cwd: ".", path: "." }, "Could not read thread workspaces.", cause),
        ),
      );
    for (const project of roots)
      for (const worktree of yield* list(project.workspaceRoot).pipe(
        Effect.catch((error) =>
          Effect.logWarning("Worktree protection could not inspect project", {
            cwd: project.workspaceRoot,
            error,
          }).pipe(Effect.as([])),
        ),
      )) {
        const input = { cwd: project.workspaceRoot, path: worktree.path };
        yield* Effect.gen(function* () {
          let referenced = knownPaths.has(worktree.path);
          for (const row of rows)
            if ((yield* canonical(path.resolve(row.worktreePath))) === worktree.path) {
              previous.set(row.threadId, input);
              knownPaths.set(worktree.path, input);
              referenced = true;
            }
          // A newly claimed checkout can precede its durable thread reference.
          if (referenced) yield* withLease(input, reconcile(input));
        }).pipe(
          Effect.catch((error) =>
            Effect.logWarning("Worktree protection could not reconcile checkout", {
              ...input,
              error,
            }),
          ),
        );
      }
  });
  const reconcileThread: WorktreeProtection["Service"]["reconcileThread"] = Effect.fn(
    "WorktreeProtection.reconcileThread",
  )(function* (threadId) {
    const old = previous.get(threadId);
    const shell = yield* projections
      .getThreadShell(threadId)
      .pipe(
        Effect.mapError((cause) =>
          failure(old ?? { cwd: ".", path: "." }, "Could not read thread workspace.", cause),
        ),
      );
    let next: WorktreeLocation | undefined;
    const worktreePath = shell?.worktreePath;
    if (shell !== null && worktreePath != null) {
      const project = yield* projects
        .get(shell.projectId, { includeDeleted: true })
        .pipe(
          Effect.mapError((cause) =>
            failure({ cwd: ".", path: worktreePath }, "Could not read thread project.", cause),
          ),
        );
      if (Option.isSome(project)) next = { cwd: project.value.workspaceRoot, path: worktreePath };
    }
    if (next === undefined) previous.delete(threadId);
    else previous.set(threadId, next);
    if (next !== undefined) yield* withLease(next, reconcile(next));
    if (old !== undefined && (next === undefined || old.path !== next.path))
      yield* withLease(old, reconcile(old));
  });
  const withReference: WorktreeProtection["Service"]["withReference"] = (input, effect) =>
    projects.get(input.projectId).pipe(
      Effect.mapError((cause) =>
        failure({ cwd: ".", path: input.worktreePath }, "Could not read thread project.", cause),
      ),
      Effect.flatMap((project) => {
        if (Option.isNone(project))
          return Effect.fail(
            failure({ cwd: ".", path: input.worktreePath }, "Thread project is unavailable."),
          );
        const location = { cwd: project.value.workspaceRoot, path: input.worktreePath };
        return withLease(location, protect(location).pipe(Effect.andThen(effect)));
      }),
    );
  return WorktreeProtection.of({
    protect,
    assertRemovable,
    reconcile,
    reconcileThread,
    reconcileAll,
    withLease,
    withReference,
  });
});
export const layer = Layer.effect(WorktreeProtection, make);
