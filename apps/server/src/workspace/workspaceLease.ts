import * as Effect from "effect/Effect";
import type * as FileSystem from "effect/FileSystem";
import type * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Semaphore from "effect/Semaphore";

/** Missing checkouts still share a key with their nearest existing path alias. */
export const canonicalWorkspacePath = (
  fileSystem: FileSystem.FileSystem,
  path: Path.Path,
  target: string,
): Effect.Effect<string, PlatformError.PlatformError> =>
  fileSystem.realPath(target).pipe(
    Effect.catch((error) => {
      if (error.reason._tag !== "NotFound") return Effect.fail(error);
      const parent = path.dirname(target);
      return parent === target
        ? Effect.succeed(target)
        : canonicalWorkspacePath(fileSystem, path, parent).pipe(
            Effect.map((real) => path.join(real, path.basename(target))),
          );
    }),
  );

const leases = new Map<string, { semaphore: Semaphore.Semaphore; users: number }>();

export const withWorkspaceLease = <A, E, R>(
  cwd: string,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.suspend(() => {
    const lease = leases.get(cwd) ?? { semaphore: Semaphore.makeUnsafe(1), users: 0 };
    leases.set(cwd, lease);
    lease.users++;
    return lease.semaphore.withPermit(effect).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          lease.users--;
          if (lease.users === 0) leases.delete(cwd);
        }),
      ),
    );
  });
