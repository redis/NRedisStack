---
name: extend-commands-api
description: Add or extend Redis / Redis Stack module commands in NRedisStack (the .NET client built on StackExchange.Redis) - a new module command (FT.*, JSON.*, TS.*, BF.*, CF.*, CMS.*, TOPK.*, TDIGEST.*), a new argument or option on an existing command, a reply that gained fields, or a core command StackExchange.Redis does not cover. Gathers evidence (HLD, server/module PR, live redis-cli probe), plans the file-by-file matrix, then implements sync + async API, command builder, command category, PublicAPI tracking and xUnit tests against standalone and cluster. Runs supervised (plan mode, user approval) by default, or unattended for automation such as the RedisClientsBot parity pipeline.
metadata:
  modes: supervised, unattended
---

# Extend the NRedisStack Commands API

Implement a new Redis command (or extend an existing one) in NRedisStack, following
the conventions maintainers enforce in review. Work evidence-first: prove the
command's behavior on a real server before designing the C# API.

## Modes - read this first

This skill runs in one of two modes. The engineering rules in every section below
are identical in both; only who answers questions and approves the plan differs.

- **Supervised** (default): a human is in the session. Ask the questions, present
  the plan in plan mode and wait for approval, as written below.
- **Unattended**: no human will answer or approve anything, so never stop to wait.
  Use it ONLY when the invoking prompt says `Mode: unattended` or the environment
  has `CLIENT_SKILL_MODE=unattended`. Never switch to it on your own.

In unattended mode the invoking prompt supplies the inputs a human would give:
the HLD path (normally `./HLD.md`), the server/module PR reference (`tracks:`),
the test command (`dotnet test tests/NRedisStack.Tests/NRedisStack.Tests.csproj -f net8.0`),
and a running standalone Redis plus a 6-node OSS cluster (3 primaries + 3
replicas) from `redislabs/client-libs-test` (modules included, no password, no
TLS, no Docker available to you). The environment carries `REDIS_URL`,
`REDIS_STANDALONE_HOST`/`REDIS_STANDALONE_PORT`, `REDIS_CLUSTER_HOST`,
`REDIS_CLUSTER_START_PORT`, `REDIS_CLUSTER_NODES`, `REDIS_CLUSTER_URLS`,
`REDIS_VERSION`, and `REDIS_ENDPOINTS_CONFIG_PATH` (an `endpoints.json` already in
NRedisStack's format with ids `standalone` and `cluster`). Wherever a step below
has an **Unattended:** note, follow the note:

| Step | Supervised | Unattended |
|---|---|---|
| Phase 0.1 HLD | ask the user for the path | read the given HLD fully |
| Phase 0.2 `gh` access | ask to run `gh` outside the sandbox | do not ask; use the `tracks:` PR and HLD facts, else the unauthenticated GitHub REST API |
| Phase 0.3 environment / image tag | `docker compose ... up -d --wait`; ask for an image tag if the command is missing | no Docker: probe `$REDIS_URL` and the cluster; if the command is missing, continue with version-gated tests |
| Phase 0.4 redis-cli scenarios | run against `127.0.0.1:6379` | run against `$REDIS_URL` (and `-c` on the cluster); if `redis-cli` is unavailable, write unverified transcripts |
| Phase 1 plan + approval | plan mode, ask before editing | write the plan into the final report, then implement it in the same session |
| Running tests | human starts compose as CI does; `dotnet test` with `--filter` | `dotnet test` with `--filter` against BOTH `standalone` and `cluster`, using the given `REDIS_ENDPOINTS_CONFIG_PATH` and `REDIS_VERSION` |
| An open design question | ask the user | take the HLD's choice (else the most consistent existing NRedisStack convention) and list it as an open question in the report |
| Experimental (`[Experimental]`) or not | ask if the HLD is unclear | experimental only if the HLD/server PR says preview / unstable-features-gated; otherwise stable |

Unattended runs also follow these rules:
- **Change files.** A run that ends without changes has failed. Stop without
  editing only when implementing is impossible (or the command belongs to
  StackExchange.Redis, see decision D), and say exactly why in the report.
- **Don't commit, push, or open a PR.** The automation that invoked you does that.
- **No CI / release / version edits**: nothing under `.github/`, no
  `tests/dockers/.env.v*` or `docker-compose.yml` image bumps, no `version.json`
  or `<Version>` changes, no `PublicAPI.Shipped.txt` promotion. Editing
  `src/NRedisStack/PublicAPI/PublicAPI.Unshipped.txt` IS required for every new
  public member. For a new experimental id only, the `Directory.Build.props`
  `NoWarn` entry is the one allowed build-file edit (see "Experimental API").
- **Finish with a plain-text report** (no placeholders) covering:
  - what was implemented, the decision-tree class (A-E) and the plan
  - design decisions, and which layers intentionally did NOT change and why
  - the command category chosen for every new/changed builder method, and why
  - unit test results, and integration results per target: the `--filter` used,
    passed/failed/skipped counts, test names, and confirmation that both the
    `standalone` and `cluster` rows ran (any skip with its reason)
  - steps skipped because they need a human or Docker
  - open questions for maintainers

  This feeds the PR description (see the PR hygiene checklist).

## Phase 0 - Gather evidence BEFORE planning

1. **Ask the user for the HLD.** Ask for the path to a markdown file with the
   High-Level Design (or confirm none exists). Read it fully - it is the primary
   source for syntax, semantics, reply shape per RESP2/RESP3, and edge cases.
   **Unattended:** don't ask; read the HLD path the invoking prompt gives.

2. **Find the server-side PR in the repo that owns the command family:**

   | Command family | Repository |
   |---|---|
   | Core commands | `redis/redis` (`src/commands/*.json` has the exact syntax) |
   | Search / query engine (`FT.*`) | `RediSearch/RediSearch` |
   | JSON (`JSON.*`) | `RedisJSON/RedisJSON` |
   | Probabilistic (`BF.*`, `CF.*`, `CMS.*`, `TOPK.*`, `TDIGEST.*`) | `RedisBloom/RedisBloom` |
   | Time series (`TS.*`) | `RedisTimeSeries/RedisTimeSeries` |

   If the routed repo has nothing, try `redis/redis` (and vice versa). Check
   `gh auth status` first; sandboxes often hide gh's credentials, so if it fails
   ask the user for permission to run these read-only `gh` commands outside the
   sandbox, and only fall back to the unauthenticated REST API if they decline.
   ```bash
   gh search prs --repo <owning-repo> "<COMMAND>" --limit 10
   gh pr view <num> --repo <owning-repo>
   gh pr diff <num> --repo <owning-repo>
   ```
   Extract: exact wire syntax (argument order, optionality), reply type per
   **RESP2 and RESP3** (module replies often become maps in RESP3), error
   conditions, whether the command is keyed / keyless / node-bound, and the
   **first server version carrying it** (RC builds such as `8.3.224` = 8.4 RC are
   valid gates). Note if the feature is *preview* or gated behind
   `search-enable-unstable-features` and the like.
   **Unattended:** don't ask for permission. Use the `tracks:` reference and the
   HLD; fill gaps from `https://api.github.com/repos/<owner>/<repo>/pulls/<num>/files`.

3. **Verify the command exists on the target server.** CI starts both topologies
   from `tests/dockers/docker-compose.yml`, selecting the image via
   `tests/dockers/.env.v<X.Y>` (pick the highest numerically: `8.10` > `8.8`):
   ```bash
   ls tests/dockers/.env.v* | sort -V | tail -1
   docker compose --env-file tests/dockers/.env.v8.10 --profile all \
     -f tests/dockers/docker-compose.yml up -d --wait
   redis-cli -p 6379 INFO server | grep redis_version
   redis-cli -p 6379 MODULE LIST                     # module present + version
   redis-cli -p 6379 COMMAND INFO <COMMAND>          # non-empty -> exists
   redis-cli -p 6379 COMMAND DOCS <COMMAND>          # args, compare with the PR
   ```
   Standalone is `127.0.0.1:6379`; the cluster is `127.0.0.1:16379-16384`
   (see `tests/dockers/endpoints.json`). For an EXTENDED command, also run the
   new syntax against a scratch key and confirm no `ERR syntax error` /
   `unknown argument`.

   **If it is missing**, the pinned image is too old. Ask the user for a
   `redislabs/client-libs-test` tag that has it (tags:
   https://hub.docker.com/r/redislabs/client-libs-test/tags - you may suggest
   candidates), then restart with
   `CLIENT_LIBS_TEST_IMAGE=redislabs/client-libs-test:<tag> docker compose --profile all -f tests/dockers/docker-compose.yml up -d --wait`
   (compose reads the full image reference). CI's custom-image job runs such
   images with `REDIS_VERSION=255.255.255`. If no image has it, report that and
   proceed: the tests will be written and gated, not run.
   **Unattended:** no Docker, no image question. Probe what you were given:
   `redis-cli -u "$REDIS_URL" COMMAND INFO <COMMAND>` and
   `redis-cli -c -h "$REDIS_CLUSTER_HOST" -p "$REDIS_CLUSTER_START_PORT" COMMAND INFO <COMMAND>`.
   If missing there, continue with version-gated tests and say so in the report.

4. **Create redis-cli showcase scenarios** from the HLD and the PR, and run them:
   - **Smoke test**: happy path, each new option, reply shape under
     `redis-cli -3` as well as RESP2, edge cases and errors (missing key/index,
     out-of-range args, conflicting options) - so parsing is built on observed
     replies. For keyless or multi-key commands, run them on the cluster too
     (`-c`); fan-out and CROSSSLOT behavior differs from standalone.
   - **Showcase**: each scenario reads as a mini use-case explaining WHY the
     command/option exists. Prefer realistic data over `foo`/`bar`.

   Keep them in a scratch file (a "what this demonstrates" comment, the
   commands, the observed reply as comments). They feed the plan, the test
   assertions and the PR description. If the command was unavailable, write them
   as *expected* transcripts and mark them unverified.
   **Unattended:** run against `$REDIS_URL` / the cluster. Keep the scratch file
   out of the change; carry the scenarios into the report.

5. **Trace one analogous existing command end to end** (same module, similar
   reply shape). Reference commits, validated against this repo's history:

   | Commit | What it exemplifies |
   |---|---|
   | `37f7662` (FT.ALIASLIST #518) | Minimal new module command: literal, builder, interface x2, impl x2, PublicAPI, sync+async theories gated `8.10.0` |
   | `f639686` (TS 8.10 #528) | New commands + new options on existing ones: `[Flags]` enum, new overloads with the old ones kept `[Obsolete]` for binary compat, `(RedisKey)` casts for cluster routing, per-command test files |
   | `d5644f5` (CMS.INFO cell size #554) | Reply gained a field: new DataTypes property (`-1` on older servers) + parser case |
   | `62e2d19` + `25f01e4` (COLLECT #524, NRS003 #536) | Option via a builder type (no interface change), then marked `[Experimental]` |
   | `87ea197` (#540) | Why every `SerializedCommand` declares a command category |

   Older code can predate current rules (for example, pre-#540 diffs construct
   `SerializedCommand` without a category). When precedent conflicts with this
   skill, the written convention wins.

## Phase 1 - Plan, then approval (supervised) or straight to implementation (unattended)

Classify the change with the decision tree, then list: the file-by-file touch
list, the proposed public signatures (sync + async) with XML docs, the command
category per builder method, the PublicAPI lines, the test matrix and the version
gate. Open with a short "what this feature enables" section with one or two
showcase transcripts.

- **Supervised:** enter **plan mode**, present the plan and **explicitly ask
  permission to execute** before editing. The public signature is the contract;
  if it must change during implementation, stop and re-confirm.
- **Unattended:** don't enter plan mode and don't wait. The merged HLD was the
  approval. Keep the plan for the final report and implement it right away,
  following the whole matrix exactly as a supervised run would.

## Decision tree - what kind of change is this?

**A. New option / argument on an existing command.**
- If the command already takes a parameter object, extend THAT and nothing else:
  Search `Query`, `FTCreateParams`, `FTSpellCheckParams`, `AggregationRequest` /
  `Reducers`, `HybridSearchQuery`; TimeSeries `TimeSeriesParamsBuilder`
  (`TsAddParamsBuilder`, ...); a `[Flags]` enum such as `TimeSeriesRangeFlags`.
  The builder already serializes the object, so interfaces and impls don't change
  (cf. COLLECT). Add the token to `Literals/CommandArgs.cs` and keep wire order.
- If the option is a positional method parameter, **do not edit the existing
  signature** (adding even an optional parameter breaks binary compatibility and
  changes a shipped PublicAPI line). Add a new overload carrying the option;
  keep the old one, marked as in `ITimeSeriesCommands.Range`:
  `[Obsolete] [Browsable(false)] [EditorBrowsable(EditorBrowsableState.Never)] [OverloadResolutionPriority(-1)]`,
  with `[OverloadResolutionPriority(1)]` on the new one, so existing calls
  resolve to the new overload. Mirror on sync + async interfaces, both impls and
  the builder. Where an integer literal could bind to the obsolete overload, add
  a compile-time guard call like `TimeSeries/TestAPI/ObsoleteOverloadResolutionGuard.cs`.
- Re-check the command category: an option can change it (JSON.SET NX/XX,
  FT.SUGADD INCR, TS.ADD ON_DUPLICATE) - pin those in `CommandCategoryTests`.

**B. Reply gained fields (INFO-style).** Extend the `DataTypes/` model
backward-compatibly: new get-only property, default for older servers (CMS used
`-1`), constructor stays `internal`; add the `case` in the parser in
`ResponseParser.cs` for RESP2 and, if it differs, RESP3 (`Resp3Type is ResultType.Map`).

**C. New command in an existing module** - the FULL matrix (FT.ALIASLIST):
1. `src/NRedisStack/<Module>/Literals/Commands.cs` - `public const string ALIASLIST = "FT.ALIASLIST";`
   in the module's `internal class` (`FT`, `JSON`, `TS`, `BF`, `CF`, `CMS`,
   `TOPK`, `TDIGEST`); sub-tokens in `Literals/CommandArgs.cs` (`SearchArgs`, ...).
2. `<Module>CommandBuilder.cs` (`TimeSeriesCommandsBuilder.cs` for TS) - a
   `public static SerializedCommand` method, args in wire order, with a category:
   ```csharp
   public static SerializedCommand AliasList(string index)
   {
       return new(CommandCategories.ReadOnly, FT.ALIASLIST, index);
   }
   ```
3. `I<Module>Commands.cs` and `I<Module>CommandsAsync.cs` - the documented
   contract (see Docs below); async name = sync name + `Async`, returning `Task<T>`.
4. `<Module>CommandsAsync.cs` - `/// <inheritdoc/>` then
   `return (await _db.ExecuteAsync(SearchCommandBuilder.AliasList(index))).ToArray();`
5. `<Module>Commands.cs` (the sync class inherits the async class) -
   `/// <inheritdoc/>` then `return db.Execute(SearchCommandBuilder.AliasList(index)).ToArray();`
6. `src/NRedisStack/PublicAPI/PublicAPI.Unshipped.txt` - one line per public
   member: both interface members, both concrete members, the static builder.
7. Parsing: reuse `ResponseParser` (`ToArray`, `ToLong`, `ToDouble`,
   `OKtoBoolean`, `ToBooleanArray`, `ToStringList`, `ToStringRedisResultDictionary`,
   ...). Add a new `internal` extension there (or in `<Module>Aux.cs`) and a
   `DataTypes/` class only when the reply is genuinely structured.
8. Pipelines/transactions need no wiring: `Pipeline` and `Transaction` expose
   the `*CommandsAsync` classes, so the async method is the batch API.

**D. New core (non-module) command.** First check StackExchange.Redis: if
`IDatabase` already has it (or will, in the referenced SE.Redis version),
NRedisStack does not wrap it - report that instead of duplicating it.
NRedisStack only covers core gaps (blocking pops, `XREAD`/`XREADGROUP` blocking,
`CLIENT SETINFO`). Those are `public static` **extension methods on
`IDatabase`** in `CoreCommands/CoreCommands.cs` + `CoreCommandsAsync.cs` (no
interface; XML docs go on the methods), built in `CoreCommandBuilder.cs`, with
literals in `CoreCommands/Literals/` (`RedisCoreCommands`, `CoreArgs`), types in
`CoreCommands/DataTypes` / `Enums`, tests in `tests/NRedisStack.Tests/Core Commands/CoreTests.cs`.

**E. New module / command group** (rare): mirror a whole module folder, add the
`db.XX()` accessor in `ModulePrefixes.cs` and a property in both `Pipeline.cs`
and `Transactions.cs`. Ask first (supervised) / list as an open question
(unattended).

## Conventions (verified in the code)

**Command category (mandatory).** Since #540 every builder passes a
`CommandFlags` category as the first `SerializedCommand` argument, using the
aliases in `RedisStackCommands/CommandCategories.cs`. The uncategorized ctors
are `[Obsolete]` and CS0618 is an error inside the library, so omitting it does
not compile. Pick by what a replay does:
`ReadOnly` (pure read) | `WriteChecked` (conditional write, a replay is a no-op:
BF.ADD, CF.ADDNX) | `WriteLastWins` (blind overwrite: JSON.SET, FT.ALIASUPDATE)
| `WriteAccumulating` (replay double-applies, and module DDL that errors on
replay: FT.CREATE, FT.ALIASADD, TS.CREATE) | `ServerAdmin` | `Connection` |
`Never` (replay corrupts or leaks: cursors, FT.CURSOR READ/DEL). OR in
`CommandCategories.ServerSpecific` for node-bound commands. When the category
depends on arguments, compute it (cf. `AggregateCategory`) and add rows to
`tests/NRedisStack.Tests/CommandCategoryTests.cs`.

**Dispatch.** Library code calls the `Auxiliary` extensions
`db.Execute(SerializedCommand)` / `db.ExecuteAsync(SerializedCommand)` - never
`db.Execute(string, ...)`, which skips the category flags and the client
SETINFO announcement.

**Keys and cluster routing.** StackExchange.Redis routes `Execute` by the
`RedisKey`-typed arguments. Take keys as `RedisKey` (or cast: `(RedisKey)key`)
when adding them to the args list - a key passed as `string` is not routed and
lands on a random primary (`-MOVED`; DEBUG builds set `NoRedirect` to surface
it). Index names, filters and other non-key names stay `string`. Keyless
fan-out commands are routed by the server/coordinator; node-bound follow-ups
(cursors) are pinned via `IServer` (see `SearchCommandsAsync`). Blocking forms
(`BLOCK`) cannot be issued on the shared multiplexer (cf. `TS.READ`, which
builds only the immediate form).

**Naming and types.** Namespace `NRedisStack` for interfaces, impls and
builders; `NRedisStack.<Module>.Literals` (TS/TDIGEST use `NRedisStack.Literals`)
for internal literals; `NRedisStack.<Module>.DataTypes` for models. PascalCase
method names without the module prefix (`AliasList`, `Card`, `ArrAppend`).
Optional args are optional parameters / nullables (`long? count = null`,
`bool latest = false`); booleans for bare flags; enums for token choices
(`[Flags]` when combinable). Nullable reference types are enabled - annotate.
The library targets `net6.0;net8.0;net10.0;netstandard2.0;net481`, so use only
APIs available on netstandard2.0 (polyfills live in `TaskPolyfills.cs`,
`OverloadResolutionPriorityAttribute.cs`, `Experiments.cs`).

**Docs.** XML docs on the interface members (both sync and async):
`<summary>`, `<param>` per parameter, `<returns>`, and
`<remarks><seealso href="https://redis.io/commands/ft.aliaslist"/></remarks>`.
Impls carry only `/// <inheritdoc/>`. Optional: `<remarks>Available since Redis X</remarks>`.
There is no `@since`-style tag, no CHANGELOG (release-drafter builds notes from
PR labels) and no per-command docs page.

**PublicAPI tracking.** `Microsoft.CodeAnalysis.PublicApiAnalyzers` reports
RS0016 (undeclared public API) / RS0017 (removed API) as **warnings, not
errors** - a green build does not prove the file is right. Build, grep the
output for `RS0016`/`RS0017`, and copy the symbol text it prints, for example
`NRedisStack.ISearchCommands.AliasList(string! index) -> StackExchange.Redis.RedisResult![]!`
and `static NRedisStack.SearchCommandBuilder.AliasList(string! index) -> NRedisStack.RedisStackCommands.SerializedCommand!`.
Add lines to `PublicAPI.Unshipped.txt` only (never edit `Shipped.txt`), then
run `python3 eng/public-api.py` to sort it (`--check` verifies; never `--promote`).

**Experimental API.** Only for preview / unstable-flag-gated server features:
add a const to `src/NRedisStack/Experiments.cs` with the next unused `NRSxxx`
id (retired ids listed there are never reused), apply
`[Experimental(Experiments.X, UrlFormat = Experiments.UrlFormat)]` to every new
public type/member, prefix each of their PublicAPI lines with `[NRSxxx]`, add
`docs/exp/NRSxxx.md`, and add the id to `NoWarn` in `Directory.Build.props`
(cf. `25f01e4`). Label the PR `experimental`.

## Test matrix - what to write

Integration test classes live in `tests/NRedisStack.Tests/<Module>/`
(`SearchTests.cs`, `JsonTests.cs`, `BloomTests.cs`, ...; TimeSeries uses one
file per command, `TimeSeries/TestAPI/Test<Cmd>.cs` + `Test<Cmd>Async.cs`).
They inherit `AbstractNRedisStackTest` with a ctor taking `EndpointsFixture`
(and optionally `ITestOutputHelper`). Write all layers that apply:

1. **Integration theories, sync AND async**, against both topologies:
   ```csharp
   [SkipIfRedisTheory(Comparison.LessThan, "8.10.0")]
   [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
   public void TestAliasList(string endpointId)
   {
       IDatabase db = GetCleanDatabase(endpointId);
       var ft = db.FT();
       ...
   }
   ```
   plus `public async Task TestAliasListAsync(string endpointId)`. Assert real
   semantics from the showcase transcripts (empty case, happy path, each option,
   server error propagated as `RedisServerException`), order-insensitively where
   the server does not promise order (`AssertKeysUnordered`). Use
   `EndpointsFixture.Env.StandaloneOnly` only when the command is meaningless on
   cluster, and say why.
2. **Gating.** `[SkipIfRedisTheory(Comparison.LessThan, "<first version>")]` (an
   RC build number is fine); `Is.Enterprise` to exclude Redis Enterprise. The
   gate reads `REDIS_VERSION` at discovery; add `deferVersionCheck: true` plus
   `AssertVersion(db)` to gate on the server's own reported version instead.
   Ungated tests use `[Theory]` - the `NRedisStack.Tests.TheoryAttribute`,
   which the test namespace picks up automatically.
3. **Cluster.** Multi-key commands need same-slot keys: `CreateKeyNames(n)`
   hash-tags them. Use the existing guards where they apply - `SkipClusterPre8`,
   `SkipClusterFanoutPre8_10` (keyless TS filter commands),
   `AliasAddOrSkipOnCrossSlot` in `SearchTests`.
4. **RESP2/RESP3 is automatic**: every theory in an `AbstractNRedisStackTest`
   subclass runs once per protocol (`[RunPerProtocol]` overrides). If a reply is
   protocol-specific, branch on `TestContext.Current.GetRunProtocol().IsResp3()`.
5. **Unit tests (no server)** for argument serialization when the builder logic
   is non-trivial: a plain class (not deriving `AbstractNRedisStackTest`)
   asserting `cmd.Args` (cf. `Search/CollectReducerTests.cs`,
   `IndexCreationTests`), and `CommandCategoryTests` rows for
   argument-dependent categories.
6. Tests compile with `WarningsAsErrors CS0612;CS0618`; a test that deliberately
   calls an obsolete overload needs `#pragma warning disable CS0612, CS0618`.

## Running the tests

`Directory.Build.props` sets `LangVersion 14`, so a **.NET 10 SDK** is needed
even for `-f net8.0`. Always pass `-f net8.0` (or net9.0/net10.0) - `net481`
cannot run on Linux. The test project copies `tests/dockers/endpoints.json` to
the output; `REDIS_ENDPOINTS_CONFIG_PATH` overrides it. A missing file fails the
fixture; an endpoint id missing from it skips that row. `REDIS_VERSION`
(default `0.0.0`) MUST be the server version or every version-gated test is
silently skipped. (CONTRIBUTING's `REDIS` / `REDIS_CLUSTER` variables are no
longer read.)

```bash
dotnet build src/NRedisStack/NRedisStack.csproj        # all TFMs: netstandard2.0 API use + RS0016
REDIS_VERSION=8.10.0 dotnet test tests/NRedisStack.Tests/NRedisStack.Tests.csproj -f net8.0 \
  --filter "FullyQualifiedName~NRedisStack.Tests.Search.SearchTests.TestAliasList" \
  --logger "console;verbosity=normal"
dotnet format                                           # CI lint gate (linter.yaml)
```
`~TestAliasList` also matches `TestAliasListAsync`. Join several tests with `|`.

- **Supervised:** the human starts Redis as CI does
  (`docker compose --env-file tests/dockers/.env.v<X.Y> --profile all -f tests/dockers/docker-compose.yml up -d --wait`),
  then run the filtered tests, and tear down with
  `docker compose --profile all -f tests/dockers/docker-compose.yml down` when done.
- **Unattended:** run the given test command with a `--filter` covering every
  new/changed test (integration and unit), with `REDIS_ENDPOINTS_CONFIG_PATH`
  and `REDIS_VERSION` exactly as provided and `--logger "console;verbosity=normal"`.
  Each theory must show a `standalone` and a `cluster` row, each for Resp2 and
  Resp3; report counts and names, and treat an unexplained skip as a failure to
  investigate (wrong `REDIS_VERSION`, missing endpoint id, CROSSSLOT guard).
  `standalone-entraid` and Enterprise tests are out of scope. Also run
  `dotnet build src/NRedisStack/NRedisStack.csproj` and `dotnet format`
  (revert any reformatting of files you did not touch).

## PR hygiene checklist (verify before finishing)

- [ ] Every layer of the chosen class updated: literal, builder, both interfaces,
      both impls (or both `CoreCommands*` files), parser/DataTypes.
- [ ] Every builder method declares the right command category; argument-dependent
      ones pinned in `CommandCategoryTests`.
- [ ] Keys passed as `RedisKey`; cluster rows of the tests pass.
- [ ] No shipped signature edited in place; old overloads kept `[Obsolete]` +
      hidden + `OverloadResolutionPriority(-1)`.
- [ ] `PublicAPI.Unshipped.txt` has every new public member, sorted with
      `eng/public-api.py`; the library build shows no RS0016/RS0017.
- [ ] XML docs with `<seealso href="https://redis.io/commands/...">` on both interfaces;
      `<inheritdoc/>` on impls.
- [ ] `[Experimental]` + `[NRSxxx]` + `docs/exp/NRSxxx.md` if (and only if) preview.
- [ ] Sync and async integration theories over `AllEnvironments`, gated at the
      first server version; `dotnet format` leaves no changes.
- [ ] PR description: server PR link, HLD link, gate version and why, category
      choices, behavior against older servers (normally the server error
      propagates unchanged - don't add client-side version checks), a showcase
      transcript. (**Unattended:** put these in the final report.)
- [ ] Docker environment stopped (supervised).

## Top pitfalls

1. **Forgetting the command category**, or picking `ReadOnly`/`WriteLastWins`
   for something that leaks, errors or double-applies on replay.
2. **Adding a key as `string`** - routes nowhere on cluster.
3. **Editing an existing public signature** (even adding an optional parameter)
   instead of adding an overload.
4. **Trusting a green build for PublicAPI** - RS0016 is only a warning here.
5. **`REDIS_VERSION` unset or wrong** - gated tests skip, and "0 failed" hides it.
6. **Only sync, or only standalone** - reviewers expect async twins and cluster rows.
7. **Copying pre-#540 code** that builds `SerializedCommand` without a category,
   or using an API missing on netstandard2.0.
8. **Wrapping a core command SE.Redis already provides.**
