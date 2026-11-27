# Instructions for AI agents working in NRedisStack

## Overview

NRedisStack is the .NET client for the Redis module commands (Search, JSON, TimeSeries, Bloom, Cuckoo,
Count-min Sketch, Top-K, T-Digest). It is a thin layer over
[StackExchange.Redis](https://github.com/seredis/StackExchange.Redis): connections, routing, pub/sub and core
commands belong to that library, not this one. For what the library does see [README.md](README.md); for the
human contribution process see [CONTRIBUTING.md](CONTRIBUTING.md).

- `src/NRedisStack/` - the library (one NuGet package). One folder per module (`Search/`, `Json/`, `TimeSeries/`,
  `Bloom/`, ...), each with sync and async interfaces, implementations, a command builder, `Literals/` and
  `DataTypes/`. `PublicAPI/` holds the tracked public surface.
- `tests/NRedisStack.Tests/` - xUnit unit and integration tests. `tests/Doc/` - the code samples published on
  redis.io. `tests/dockers/` - the Redis test topology (compose file, per-version `.env.v*`, `endpoints.json`).
- `Examples/` - user-facing markdown examples. `docs/exp/` - one page per `[Experimental]` diagnostic id.
- `eng/public-api.py` - sorts the PublicAPI tracking files. `.agents/skills/` - task procedures (see Skills).

## Commands

A .NET 10 SDK is required for every target framework (`LangVersion` is 14). The default branch is `master`.

```bash
# install / restore
dotnet restore

# build the library on all target frameworks (catches netstandard2.0 API misuse and PublicAPI warnings)
dotnet build src/NRedisStack/NRedisStack.csproj

# build everything
dotnet build

# format, which is also the lint gate (CI fails if this changes any file)
dotnet format

# check the PublicAPI files are sorted (drop --check to sort them)
python3 eng/public-api.py --check

# start the test Redis: standalone on 6379 and a 6-node cluster on 16379-16384
docker compose --env-file tests/dockers/.env.v8.10 --profile all -f tests/dockers/docker-compose.yml up -d --wait

# run all tests on one framework (net481 does not run on Linux)
REDIS_VERSION=8.10.0 dotnet test tests/NRedisStack.Tests/NRedisStack.Tests.csproj -f net10.0

# run one test (~ is a substring match, so this also runs TestAliasListAsync)
REDIS_VERSION=8.10.0 dotnet test tests/NRedisStack.Tests/NRedisStack.Tests.csproj -f net10.0 \
  --filter "FullyQualifiedName~NRedisStack.Tests.Search.SearchTests.TestAliasList"

# stop the test Redis
docker compose --profile all -f tests/dockers/docker-compose.yml down
```

There is no separate typecheck step: the compiler does it, with nullable reference types enabled.

## Repo conventions

- **`REDIS_VERSION` must match the running server.** It defaults to `0.0.0`, which silently skips every
  version-gated test, so "0 failed" proves nothing. Read the skipped count.
- **A green build does not prove the public API files are right.** RS0016 (undeclared API) and RS0017 (removed
  API) are warnings here. Search the build output for them. Add new lines to
  `src/NRedisStack/PublicAPI/PublicAPI.Unshipped.txt` only, then sort with `eng/public-api.py`. Never edit
  `PublicAPI.Shipped.txt` and never run `--promote`; that is a release step.
- **Never change a shipped public signature**, not even by adding an optional parameter: it is a binary break
  for already-compiled callers. Add an overload and keep the old one working, marked `[Obsolete]`.
- **Every command declares a command category.** Builders pass a `CommandCategories` value as the first
  `SerializedCommand` argument. The uncategorized constructors are obsolete and that is an error inside the
  library. The category decides whether a command may be retried, so choose it by what a replay would do.
- **Dispatch through `db.Execute(SerializedCommand)`** (the `Auxiliary` extensions), never
  `db.Execute(string, ...)`, which bypasses the category and the client `SETINFO` announcement.
- **Keys are `RedisKey`, not `string`.** StackExchange.Redis routes by `RedisKey`-typed arguments, so a key
  passed as a string is not routed and fails on cluster. Index names and other non-key names stay `string`.
- **Sync and async move together.** Each module has `I<Module>Commands` and `I<Module>CommandsAsync` with their
  implementations. A change to one needs the same change and the same tests in the other.
- **Integration tests run on standalone and cluster.** Use theories over `EndpointsFixture.Env.AllEnvironments`
  and gate on server version with `SkipIfRedisTheory`. An endpoint id missing from `endpoints.json` skips that
  row rather than failing it.
- **Use only APIs available on `netstandard2.0`.** The library targets
  `net6.0;net8.0;net10.0;netstandard2.0;net481`. Polyfills live in `TaskPolyfills.cs`,
  `OverloadResolutionPriorityAttribute.cs` and `Experiments.cs`.
- **Experimental ids are never reused.** `[Experimental]` ids (`NRSxxx`) are defined in
  `src/NRedisStack/Experiments.cs`, which also lists the retired ones. Each id has a page in `docs/exp/`.
- **Core commands belong upstream.** If StackExchange.Redis already exposes a command, do not wrap it here.
- **Do not add client-side server-version checks.** Against an older server the server's error propagates
  unchanged.
- **`README.md` ships inside the NuGet package**, so its links must be absolute URLs.
- There is no changelog file. Release notes are generated from PR labels by release-drafter.

## Boundaries

Always:

1. Make the change in both the sync and async surface, and test both, on standalone and cluster.
2. Run `dotnet format`, build the library on all target frameworks, and run the focused tests before calling a
   change done. Report skipped tests and the reason for each, not only failures.

Ask first:

1. Adding or upgrading a dependency.
2. Editing anything under `.github/`, the test images in `tests/dockers/`, `version.json` or a `<Version>`
   property.
3. Any change to shipped public API, defaults or error behavior.
4. Adding a new `[Experimental]` id.

Never:

1. Weaken, skip or delete a test to make a change pass. If a test is wrong or obsolete, say so in the PR
   description and let a human decide.
2. Add `Signed-off-by`, `Co-Authored-By` or "Generated with" lines to commits or PR text. The human is the
   author of record.
3. Commit, push, create or merge PRs, or post comments, reviews or issues without a specific instruction from
   the user for that action. Draft the text and let a human post it. This does not apply when a maintainer
   started the agent for that purpose.
4. Modify unrelated code, including reformatting files you did not otherwise touch.

## PR conventions

The full process is in [CONTRIBUTING.md](CONTRIBUTING.md#pull-request-process). The rules to follow every time:

- PRs are squash-merged, so the PR title becomes the commit message on `master`. Write it as one sentence about
  the user-visible change, for example `Support WITHCOUNT / WITHOUTCOUNT on FT.SEARCH and FT.AGGREGATE`.
- Fill in the PR template, including the AI disclosure, and include tests and XML docs with the change.
- Link the issue the PR fixes, for example `Fixes #1234`. For a new server feature, link the server-side PR and
  state the first Redis version that carries it.

## Skills

Task procedures live in `.agents/skills/`; each is symlinked into `.claude/skills/`. Use the skill when the
task matches.

| Skill | Use when |
|---|---|
| `extend-commands-api` | Adding or extending a Redis module command, option or reply field |
| `create-implementation-plan-for-redis-api-change` | Writing the implementation plan for a Redis API change from a shared client HLD, before any code |

A new or extended command is not done until every item in the `extend-commands-api` PR hygiene checklist holds.
