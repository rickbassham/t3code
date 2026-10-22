import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Result from "effect/Result";
import { expect } from "vite-plus/test";

import { canonicalWorkspacePath } from "./workspaceLease.ts";

it.layer(NodeServices.layer)("workspace lease paths", (it) => {
  it.effect("canonicalizes missing checkouts below a symlinked ancestor", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const temporary = yield* fs.makeTempDirectoryScoped({ prefix: "workspace-lease-" });
      const target = yield* fs.realPath(temporary);
      const alias = path.join(temporary, "alias");
      yield* fs.symlink(target, alias);
      expect(yield* canonicalWorkspacePath(fs, path, path.join(alias, "missing", "checkout"))).toBe(
        path.join(target, "missing", "checkout"),
      );
    }),
  );

  it.effect("propagates filesystem failures instead of using a different lease key", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const failure = PlatformError.systemError({
        _tag: "PermissionDenied",
        module: "FileSystem",
        method: "realPath",
        pathOrDescriptor: "/denied",
      });
      const result = yield* canonicalWorkspacePath(
        {
          ...fs,
          realPath: () => Effect.fail(failure),
        },
        path,
        "/denied",
      ).pipe(Effect.result);
      expect(result).toEqual(Result.fail(failure));
    }),
  );
});
