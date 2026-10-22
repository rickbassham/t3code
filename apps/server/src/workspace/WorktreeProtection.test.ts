import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { GitCommandError, ProjectId, ThreadId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as ServerConfig from "../config.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as WorktreeProtection from "./WorktreeProtection.ts";
import { WORKTREE_PROTECTION_REASON } from "./worktreeProtectionReason.ts";

const layer = GitVcsDriver.layer.pipe(
  Layer.provideMerge(VcsProcess.layer),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "worktree-protection-" })),
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provideMerge(NodeServices.layer),
);

const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const config = yield* ServerConfig.ServerConfig;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "protection-git-" });
  const cwd = `${root}/repo`;
  const managedRepo = `${config.worktreesDir}/${root.split("/").at(-1)}`;
  const path = `${managedRepo}/shared`;
  yield* fs.makeDirectory(cwd, { recursive: true });
  yield* fs.makeDirectory(managedRepo, { recursive: true });
  const command = (args: ReadonlyArray<string>) =>
    git.execute({ operation: "WorktreeProtection.test", cwd, args });
  yield* command(["init", "-b", "main"]);
  yield* command([
    "-c",
    "user.name=Test",
    "-c",
    "user.email=test@example.test",
    "-c",
    "commit.gpgsign=false",
    "commit",
    "--allow-empty",
    "-m",
    "initial",
  ]);
  yield* command(["worktree", "add", "-b", "shared", path]);
  const references = yield* Ref.make<ReadonlyArray<{ threadId: ThreadId; worktreePath: string }>>(
    [],
  );
  const historical = yield* Ref.make<ReadonlyArray<{ threadId: ThreadId; worktreePath: string }>>(
    [],
  );
  const projectId = ProjectId.make("project:protection");
  const project = {
    projectId,
    title: "Protection fixture",
    workspaceRoot: cwd,
    defaultModelSelection: null,
    defaultThreadEnvMode: null,
    autoPull: false,
    faviconPath: null,
    projectIcon: null,
    scripts: [],
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    deletedAt: null,
  } satisfies ProjectStore.ProjectRow;
  const shell = {
    ...project,
    id: projectId,
    repositoryIdentity: null,
  };
  const failedLockPath = yield* Ref.make<string | null>(null);
  const service = yield* WorktreeProtection.make.pipe(
    Effect.provideService(GitVcsDriver.GitVcsDriver, {
      ...git,
      execute: (input) =>
        Ref.get(failedLockPath).pipe(
          Effect.flatMap((failed) =>
            input.args[0] === "worktree" && input.args[1] === "lock" && input.args.at(-1) === failed
              ? Effect.fail(
                  new GitCommandError({
                    operation: input.operation,
                    cwd: input.cwd,
                    command: "git worktree lock",
                    detail: "Injected lock failure",
                  }),
                )
              : git.execute(input),
          ),
        ),
    }),
    Effect.provide(
      Layer.merge(
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadWorktreePaths: (options) =>
            options !== undefined && "includeDeleted" in options && options.includeDeleted === true
              ? Effect.all([Ref.get(references), Ref.get(historical)]).pipe(
                  Effect.map(([active, old]) => [...active, ...old]),
                )
              : Ref.get(references),
        }),
        Layer.mock(ProjectStore.ProjectStoreV2)({
          get: () => Effect.succeed(Option.some(project)),
          listShells: () =>
            Effect.succeed([
              { ...shell, id: ProjectId.make("project:missing"), workspaceRoot: `${root}/missing` },
              shell,
            ]),
        }),
      ),
    ),
  );
  const location = { cwd, path };
  return {
    fs,
    config,
    cwd,
    path,
    command,
    references,
    historical,
    projectId,
    service,
    location,
    failedLockPath,
  };
});

it.layer(layer)("WorktreeProtection", (it) => {
  it.effect("continues protecting other checkouts after one checkout lock fails", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const other = `${f.path}-other`;
      yield* f.command(["worktree", "add", "-b", "other", other]);
      yield* Ref.set(f.references, [
        { threadId: ThreadId.make("thread:first"), worktreePath: f.path },
        { threadId: ThreadId.make("thread:other"), worktreePath: other },
      ]);
      yield* Ref.set(f.failedLockPath, yield* f.fs.realPath(f.path));
      yield* f.service.reconcileAll;
      const listed = (yield* f.command(["worktree", "list", "--porcelain"])).stdout;
      assert.include(listed, `worktree ${yield* f.fs.realPath(other)}`);
      yield* f.command(["worktree", "remove", "--force", other]).pipe(Effect.flip);
      assert.isTrue(yield* f.fs.exists(other));
      yield* f.command(["worktree", "remove", f.path]);
    }).pipe(Effect.scoped),
  );

  it.effect("protects an existing checkout against ordinary Git removal", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.service.withLease(f.location, f.service.protect(f.location));
      const removal = yield* f.command(["worktree", "remove", "--force", f.path]).pipe(Effect.flip);
      assert.equal(removal._tag, "GitCommandError");
      assert.isTrue(yield* f.fs.exists(f.path));
      assert.include(
        (yield* f.command(["worktree", "list", "--porcelain"])).stdout,
        `locked ${WORKTREE_PROTECTION_REASON}`,
      );
    }).pipe(Effect.scoped),
  );

  it.effect("keeps shared references protected until the final reference retires", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const first = { threadId: ThreadId.make("thread:first"), worktreePath: f.path };
      const second = { threadId: ThreadId.make("thread:second"), worktreePath: f.path };
      yield* Ref.set(f.references, [first, second]);
      yield* f.service.reconcile(f.location);
      yield* Ref.set(f.references, [second]);
      yield* f.service.reconcile(f.location);
      const guarded = yield* f.service.assertRemovable(f.location).pipe(Effect.flip);
      assert.equal(guarded._tag, "GitCommandError");
      yield* f.command(["worktree", "remove", "--force", f.path]).pipe(Effect.flip);
      yield* Ref.set(f.references, []);
      yield* f.service.reconcile(f.location);
      yield* f.service.assertRemovable(f.location);
      yield* f.command(["worktree", "remove", f.path]);
      assert.isFalse(yield* f.fs.exists(f.path));
    }).pipe(Effect.scoped),
  );

  it.effect("serializes attachment against removal and checks the newly committed reference", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const attaching = yield* Deferred.make<void>();
      const commit = yield* Deferred.make<void>();
      const removing = yield* Deferred.make<void>();
      const enteredRemoval = yield* Ref.make(false);
      const attachment = yield* f.service
        .withReference(
          { projectId: f.projectId, worktreePath: f.path },
          Deferred.succeed(attaching, undefined).pipe(
            Effect.andThen(Deferred.await(commit)),
            Effect.andThen(
              Ref.set(f.references, [
                { threadId: ThreadId.make("thread:attaching"), worktreePath: f.path },
              ]),
            ),
          ),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(attaching);
      const removal = yield* Deferred.succeed(removing, undefined).pipe(
        Effect.andThen(
          f.service.withLease(
            f.location,
            Ref.set(enteredRemoval, true).pipe(
              Effect.andThen(f.service.assertRemovable(f.location)),
              Effect.andThen(f.command(["worktree", "remove", "--force", f.path])),
            ),
          ),
        ),
        Effect.flip,
        Effect.forkChild,
      );
      yield* Deferred.await(removing);
      assert.isFalse(yield* Ref.get(enteredRemoval));
      yield* Deferred.succeed(commit, undefined);
      yield* Fiber.join(attachment);
      const error = yield* Fiber.join(removal);
      assert.equal(error._tag, "GitCommandError");
      assert.isTrue(yield* Ref.get(enteredRemoval));
      assert.isTrue(yield* f.fs.exists(f.path));
    }).pipe(Effect.scoped),
  );

  it.effect(
    "recovers retired locks at startup without unlocking a checkout awaiting its first reference",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture;
        const fresh = `${f.path}-fresh`;
        const active = `${f.path}-active`;
        yield* f.command(["worktree", "add", "-b", "fresh", fresh]);
        yield* f.command(["worktree", "add", "-b", "active", active]);
        yield* Ref.set(f.references, [
          { threadId: ThreadId.make("thread:active"), worktreePath: active },
        ]);
        yield* f.service.protect(f.location);
        yield* f.service.protect({ cwd: f.cwd, path: fresh });
        yield* Ref.set(f.historical, [
          { threadId: ThreadId.make("thread:deleted"), worktreePath: f.path },
        ]);
        yield* f.service.reconcileAll;
        yield* f.command(["worktree", "remove", f.path]);
        const refusal = yield* f
          .command(["worktree", "remove", "--force", fresh])
          .pipe(Effect.flip);
        assert.equal(refusal._tag, "GitCommandError");
        assert.isTrue(yield* f.fs.exists(fresh));
        yield* f.command(["worktree", "remove", "--force", active]).pipe(Effect.flip);
        assert.isTrue(yield* f.fs.exists(active));
      }).pipe(Effect.scoped),
  );

  it.effect("preserves a user's lock after all references retire", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.command(["worktree", "lock", "--reason", "my manual lock", f.path]);
      yield* f.service.protect(f.location);
      yield* f.service.reconcile(f.location);
      assert.include(
        (yield* f.command(["worktree", "list", "--porcelain"])).stdout,
        "locked my manual lock",
      );
      yield* f.command(["worktree", "remove", "--force", f.path]).pipe(Effect.flip);
      assert.isTrue(yield* f.fs.exists(f.path));
    }).pipe(Effect.scoped),
  );

  it.effect("does not lock project roots or unmanaged linked checkouts", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const unmanaged = `${f.cwd}-external`;
      yield* f.command(["worktree", "add", "-b", "external", unmanaged]);
      yield* f.service.protect({ cwd: f.cwd, path: f.cwd });
      yield* f.service.protect({ cwd: f.cwd, path: unmanaged });
      const nonGit = `${f.cwd}-non-git`;
      yield* f.fs.makeDirectory(nonGit);
      yield* f.service.protect({ cwd: nonGit, path: nonGit });
      const listing = (yield* f.command(["worktree", "list", "--porcelain"])).stdout;
      assert.notInclude(listing, "locked");
      yield* f.command(["worktree", "remove", unmanaged]);
    }).pipe(Effect.scoped),
  );
});
