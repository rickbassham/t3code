import { assert, expect, it, vi } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { GitCommandError, VcsRepositoryDetectionError } from "@t3tools/contracts";

import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";

import * as WorktreeProtection from "../workspace/WorktreeProtection.ts";
import * as VcsDriver from "../vcs/VcsDriver.ts";
import * as WorktreeProtectionTestkit from "../workspace/WorktreeProtection.testkit.ts";
import * as ServerConfig from "../config.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as GitManager from "./GitManager.ts";
import * as GitWorkflowService from "./GitWorkflowService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";

function layer(input: {
  readonly detect: VcsDriverRegistry.VcsDriverRegistry["Service"]["detect"];
}) {
  return GitWorkflowService.layer.pipe(
    Layer.provide(WorktreeProtectionTestkit.layer),
    Layer.provide(
      Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({
        detect: input.detect,
      }),
    ),
    Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
    Layer.provide(Layer.mock(GitManager.GitManager)({})),
  );
}

it.layer(NodeServices.layer)("GitWorkflowService", (it) => {
  it.effect.each([true, false])("guards worktree removal with active reference %s", (active) =>
    Effect.gen(function* () {
      let leased = false;
      const events: string[] = [];
      const removed = vi.fn();
      const resolved = vi.fn();
      const location = { cwd: "/repo", path: "/worktrees/repo/shared", force: true };
      const driver = yield* VcsDriver.VcsDriver;
      const protectionLayer = Layer.mock(WorktreeProtection.WorktreeProtection)({
        withLease: (_input, effect) =>
          Effect.gen(function* () {
            leased = true;
            events.push("lease");
            return yield* effect.pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  leased = false;
                  events.push("release");
                }),
              ),
            );
          }),
        assertRemovable: () =>
          Effect.gen(function* () {
            assert.isTrue(leased);
            events.push("guard");
            if (active)
              return yield* new GitCommandError({
                operation: "WorktreeProtection.assertRemovable",
                cwd: location.cwd,
                command: "git worktree remove",
                detail: "A resumable thread references the checkout.",
              });
          }),
      });
      const workflowLayer = GitWorkflowService.layer.pipe(
        Layer.provide(protectionLayer),
        Layer.provide(
          Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({
            resolve: () =>
              Effect.sync(() => {
                resolved();
                assert.isTrue(leased);
                events.push("resolve");
                return {
                  kind: "git",
                  driver,
                  repository: {
                    kind: "git",
                    rootPath: location.cwd,
                    metadataPath: "/repo/.git",
                    freshness: {
                      source: "live-local",
                      observedAt: DateTime.makeUnsafe("2026-01-01T00:00:00.000Z"),
                      expiresAt: Option.none(),
                    },
                  },
                } satisfies VcsDriverRegistry.VcsDriverHandle;
              }),
          }),
        ),
        Layer.provide(
          Layer.mock(GitVcsDriver.GitVcsDriver)({
            removeWorktree: () =>
              Effect.sync(() => {
                assert.isTrue(leased);
                events.push("remove");
                removed();
              }),
          }),
        ),
        Layer.provide(Layer.mock(GitManager.GitManager)({})),
      );
      yield* Effect.gen(function* () {
        const workflow = yield* GitWorkflowService.GitWorkflowService;
        if (active) {
          const error = yield* workflow.removeWorktree(location).pipe(Effect.flip);
          assert.equal(error._tag, "GitCommandError");
          expect(resolved).not.toHaveBeenCalled();
          expect(removed).not.toHaveBeenCalled();
          assert.deepEqual(events, ["lease", "guard", "release"]);
        } else {
          yield* workflow.removeWorktree(location);
          expect(removed).toHaveBeenCalledOnce();
          assert.deepEqual(events, ["lease", "guard", "resolve", "remove", "release"]);
        }
        assert.isFalse(leased);
      }).pipe(Effect.provide(workflowLayer));
    }).pipe(
      Effect.provide(
        Layer.mock(VcsDriver.VcsDriver)({
          capabilities: {
            kind: "git",
            supportsWorktrees: true,
            supportsBookmarks: false,
            supportsAtomicSnapshot: false,
            supportsPushDefaultRemote: true,
            ignoreClassifier: "git-compatible-fallback",
          },
        }),
      ),
    ),
  );

  it.effect("reports a non-Git VCS repository as not a Git repository", () =>
    Effect.gen(function* () {
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const isRepository = yield* workflow.isRepository("/jj-repo");

      assert.equal(isRepository, false);
    }).pipe(
      Effect.provide(
        layer({
          detect: () =>
            Effect.succeed({
              kind: "jj",
              repository: {
                kind: "jj",
                rootPath: "/jj-repo",
                metadataPath: "/jj-repo/.jj",
                freshness: {
                  source: "live-local",
                  observedAt: DateTime.makeUnsafe("2026-01-01T00:00:00.000Z"),
                  expiresAt: Option.none(),
                },
              },
              driver: {} as VcsDriverRegistry.VcsDriverHandle["driver"],
            }),
        }),
      ),
    ),
  );

  it.effect("returns an empty local status when no VCS repository is detected", () =>
    Effect.gen(function* () {
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const status = yield* workflow.localStatus({ cwd: "/not-a-repo" });

      assert.deepStrictEqual(status, {
        isRepo: false,
        hasPrimaryRemote: false,
        isDefaultRef: false,
        refName: null,
        hasWorkingTreeChanges: false,
        workingTree: {
          files: [],
          insertions: 0,
          deletions: 0,
        },
      });
    }).pipe(
      Effect.provide(
        layer({
          detect: () => Effect.succeed(null),
        }),
      ),
    ),
  );

  it.effect("returns an empty full status when no VCS repository is detected", () =>
    Effect.gen(function* () {
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const status = yield* workflow.status({ cwd: "/not-a-repo" });

      assert.deepStrictEqual(status, {
        isRepo: false,
        hasPrimaryRemote: false,
        isDefaultRef: false,
        refName: null,
        hasWorkingTreeChanges: false,
        workingTree: {
          files: [],
          insertions: 0,
          deletions: 0,
        },
        hasUpstream: false,
        aheadCount: 0,
        behindCount: 0,
        aheadOfDefaultCount: 0,
        pr: null,
      });
    }).pipe(
      Effect.provide(
        layer({
          detect: () => Effect.succeed(null),
        }),
      ),
    ),
  );

  it.effect("does not call GitManager status methods when no VCS repository is detected", () => {
    const localStatus = vi.fn();
    const remoteStatus = vi.fn();
    const status = vi.fn();

    const layerTest = GitWorkflowService.layer.pipe(
      Layer.provide(WorktreeProtectionTestkit.layer),
      Layer.provide(
        Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({
          detect: () => Effect.succeed(null),
        }),
      ),
      Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
      Layer.provide(
        Layer.mock(GitManager.GitManager)({
          localStatus,
          remoteStatus,
          status,
        }),
      ),
    );

    return Effect.gen(function* () {
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      yield* workflow.localStatus({ cwd: "/not-a-repo" });
      yield* workflow.remoteStatus({ cwd: "/not-a-repo" });
      yield* workflow.status({ cwd: "/not-a-repo" });

      assert.equal(localStatus.mock.calls.length, 0);
      assert.equal(remoteStatus.mock.calls.length, 0);
      assert.equal(status.mock.calls.length, 0);
    }).pipe(Effect.provide(layerTest));
  });

  it.effect("returns an empty ref list when no VCS repository is detected", () =>
    Effect.gen(function* () {
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const refs = yield* workflow.listRefs({ cwd: "/not-a-repo" });

      assert.deepStrictEqual(refs, {
        refs: [],
        isRepo: false,
        hasPrimaryRemote: false,
        nextCursor: null,
        totalCount: 0,
      });
    }).pipe(
      Effect.provide(
        layer({
          detect: () => Effect.succeed(null),
        }),
      ),
    ),
  );

  it.effect("structures workflow detection failures without exposing upstream details", () => {
    const cause = new VcsRepositoryDetectionError({
      operation: "VcsDriverRegistry.detect",
      cwd: "/repo",
      detail: "upstream detail must stay in the cause chain",
    });

    return Effect.gen(function* () {
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const error = yield* workflow.status({ cwd: "/repo" }).pipe(Effect.flip);

      expect(error).toMatchObject({
        _tag: "GitManagerError",
        operation: "GitWorkflowService.status",
        cwd: "/repo",
        detail: "Failed to detect a VCS repository for this Git workflow.",
      });
      expect(error.message).not.toContain(cause.detail);
    }).pipe(
      Effect.provide(
        layer({
          detect: () => Effect.fail(cause),
        }),
      ),
    );
  });

  it.effect("structures command detection failures without exposing upstream details", () => {
    const cause = new VcsRepositoryDetectionError({
      operation: "VcsDriverRegistry.detect",
      cwd: "/repo",
      detail: "upstream command detail must stay in the cause chain",
    });

    return Effect.gen(function* () {
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const error = yield* workflow.listRefs({ cwd: "/repo" }).pipe(Effect.flip);

      expect(error).toMatchObject({
        _tag: "GitCommandError",
        operation: "GitWorkflowService.listRefs",
        command: "vcs-route",
        cwd: "/repo",
        detail: "Failed to detect a VCS repository for this Git command.",
      });
      expect(error.message).not.toContain(cause.detail);
    }).pipe(
      Effect.provide(
        layer({
          detect: () => Effect.fail(cause),
        }),
      ),
    );
  });

  it.effect("finds a name among the refs of every namespace", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-ref-named-" });
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const driver = yield* GitVcsDriver.GitVcsDriver;
      for (const args of [
        ["init", "-b", "main"],
        ["-c", "user.email=t@t", "-c", "user.name=t", "commit", "--allow-empty", "-m", "init"],
        ["update-ref", "refs/remotes/upstream/feature/topic", "HEAD"],
        ["symbolic-ref", "refs/remotes/origin/alias", "refs/heads/main"],
        ["update-ref", "refs/mirror/origin/mirrored", "HEAD"],
      ]) {
        yield* driver.execute({ operation: "test.setupRepo", cwd, args });
      }
      const found = yield* Effect.forEach(
        ["main", "feature/topic", "alias", "mirrored", "t3/renamed", "feature/*"],
        (refName) => workflow.hasRefNamed({ cwd, refName }),
      );
      assert.deepEqual(found, [true, true, true, true, false, false]);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        GitWorkflowService.layer.pipe(
          Layer.provide(WorktreeProtectionTestkit.layer),
          Layer.provide(
            Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({
              resolve: () => Effect.succeed({ kind: "git" } as VcsDriverRegistry.VcsDriverHandle),
            }),
          ),
          Layer.provide(Layer.mock(GitManager.GitManager)({})),
          Layer.provideMerge(GitVcsDriver.layer),
          Layer.provideMerge(VcsProcess.layer),
          Layer.provideMerge(
            ServerConfig.layerTest(process.cwd(), { prefix: "t3-ref-named-" }).pipe(
              Layer.provide(NodeServices.layer),
            ),
          ),
          Layer.provideMerge(NodeServices.layer),
        ),
      ),
    ),
  );
});
