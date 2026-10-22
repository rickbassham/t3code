import * as Path from "effect/Path";
import { withWorkspaceLease } from "./workspaceLease.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as WorktreeProtection from "./WorktreeProtection.ts";

/** Unrelated orchestration tests isolate Git protection at its service boundary. */
export const layer = Layer.effect(
  WorktreeProtection.WorktreeProtection,
  Effect.gen(function* () {
    const path = yield* Path.Path;
    return WorktreeProtection.WorktreeProtection.of({
      protect: () => Effect.void,
      assertRemovable: () => Effect.void,
      reconcile: () => Effect.void,
      reconcileThread: () => Effect.void,
      reconcileAll: Effect.void,
      withReference: (_input, effect) => effect,
      withLease: (input, effect) => withWorkspaceLease(path.resolve(input.cwd, input.path), effect),
    });
  }),
).pipe(Layer.provide(Path.layer));
