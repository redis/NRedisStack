# Contributing

We appreciate your interest in contributing to NRedisStack. Community contributions mean a lot to us, whether
that is a fix for a bug you hit, a feature your team needs, better documentation, or help triaging issues.

If you do not know where to start, look through the
[help-wanted issues](https://github.com/redis/NRedisStack/labels/help-wanted).

## Before you start

For anything bigger than a small bug fix, a typo or a docs change, open an
[issue](https://github.com/redis/NRedisStack/issues) or a
[discussion](https://github.com/redis/NRedisStack/discussions) first. Describe the problem and the change you
plan, and wait for a maintainer to agree with the approach before you write the code. This avoids work on
changes we cannot accept, for example one that breaks the public API or duplicates work already in progress.

Search the open issues and pull requests first; someone may already be working on the same thing.

NRedisStack covers the Redis module commands. Connections, routing, pub/sub and core commands belong to
[StackExchange.Redis](https://github.com/seredis/StackExchange.Redis), so a change to those goes there.

When you file a bug, answer these five questions:

1. What version of NRedisStack are you using?
2. What version of Redis are you using?
3. What did you do?
4. What did you expect to see?
5. What did you see instead?

## Development setup

You need:

- A **.NET 10 SDK**. The projects use C# 14, so it is needed whichever target framework you build.
- **Docker**, to run the Redis servers the integration tests talk to.
- Python 3, only if you change the public API (see [Code style and linting](#code-style-and-linting)).

Fork the repository, clone your fork, and build:

```bash
dotnet restore
dotnet build
```

### Starting Redis

The tests use two topologies, both started from one compose file: a standalone server on `6379` and a six-node
cluster (three primaries, three replicas) on `16379`-`16384`. The image includes the modules.

```bash
docker compose --env-file tests/dockers/.env.v8.10 --profile all -f tests/dockers/docker-compose.yml up -d --wait
```

- `--env-file` picks the Redis version. There is one `tests/dockers/.env.v<X.Y>` per version CI tests.
- `--profile` is `all`, `standalone` or `cluster`.
- To try an image that has no env file, set it directly:
  `CLIENT_LIBS_TEST_IMAGE=redislabs/client-libs-test:<tag> docker compose --profile all -f tests/dockers/docker-compose.yml up -d --wait`

Stop it with:

```bash
docker compose --profile all -f tests/dockers/docker-compose.yml down
```

There is no sentinel topology in the test suite.

The repository also has a [dev container](.devcontainer/) definition. It predates the compose topology above
and targets older .NET and Redis Stack images, so prefer the steps in this section.

## Running tests

```bash
REDIS_VERSION=8.10.0 dotnet test tests/NRedisStack.Tests/NRedisStack.Tests.csproj -f net10.0
```

Always pass a framework with `-f` (`net8.0`, `net9.0` or `net10.0`); `net481` runs only on Windows.

**Unit and integration tests** live in the same project. Unit tests need no server. Integration tests are
theories that run once per topology, so each shows a `standalone` row and a `cluster` row.

**A single test.** Filter by name. `~` is a substring match, so this also runs `TestAliasListAsync`:

```bash
REDIS_VERSION=8.10.0 dotnet test tests/NRedisStack.Tests/NRedisStack.Tests.csproj -f net10.0 \
  --filter "FullyQualifiedName~NRedisStack.Tests.Search.SearchTests.TestAliasList"
```

**Environment variables.**

| Variable | Meaning |
|---|---|
| `REDIS_VERSION` | The version of the server you are testing against. Tests for newer commands are gated on it. It defaults to `0.0.0`, which skips every gated test without failing, so always set it and check the skipped count. |
| `REDIS_ENDPOINTS_CONFIG_PATH` | Path to an endpoints file, to test against servers other than the compose ones. It defaults to [`tests/dockers/endpoints.json`](tests/dockers/endpoints.json), which is copied next to the test binaries. A topology missing from the file is skipped. |
| `IS_ENTERPRISE` | Set to `true` when the server is Redis Enterprise, which skips tests that do not apply there. |

In an IDE, set these in [.runsettings](.vscode/.runsettings).

`tests/Doc/` holds the code samples published on redis.io. They are compiled and run as tests, so keep them
working when you change the API they use.

## Code style and linting

Formatting is checked in CI by running `dotnet format` and failing if it changes anything. Run it before you
push:

```bash
dotnet format
```

Other rules the build or the reviewers enforce:

- **Public API is tracked.** Every public member is listed in `src/NRedisStack/PublicAPI/`. Add new members to
  `PublicAPI.Unshipped.txt` (the build prints the exact line in an `RS0016` warning; it is a warning, not an
  error, so look for it), then sort the file with `python3 eng/public-api.py`. Do not edit
  `PublicAPI.Shipped.txt`.
- **No breaking changes to shipped API.** Adding a parameter to an existing method, even an optional one,
  breaks callers compiled against the previous version. Add an overload and mark the old one `[Obsolete]`.
- **Sync and async together.** Every command exists on both the sync and the async interface.
- **XML docs** on new interface members, with a link to the command's page on redis.io.
- Nullable reference types are on. The library also targets `netstandard2.0`, so use only APIs available there.

## Pull request process

- **Title.** We squash-merge pull requests, so the PR title becomes the commit message on `master`. Write it as
  one sentence that describes the user-visible change, for example
  `Support WITHCOUNT / WITHOUTCOUNT on FT.SEARCH and FT.AGGREGATE`.
- **Description.** Fill in the PR template. Explain what changed and why, and link the issue the PR fixes, for
  example `Fixes #1234`. For a new server feature, link the server-side change and say which Redis version
  first has it.
- **Tests.** Every behavior change needs tests, for the sync and async API, on standalone and cluster where the
  command applies to both.
- **Changelog.** There is no changelog file to edit. Maintainers label each PR, and the release notes are
  generated from the merged PRs and their labels.
- **Compatibility.** If your change breaks the public API, say so in the description. Expect it to be reworked
  into an additive change or to wait for a major release.

### Code review

The core team regularly looks at pull requests and will give feedback as soon as possible. Reviewers check that
the change is correct, matches Redis semantics, works on cluster, keeps the sync and async API aligned, does
not break the public API, and is tested and documented.

After receiving our feedback, please respond within two weeks. After that time, we may close your PR if it is
not showing any activity. Separately, the [stale workflow](.github/workflows/stale.yml) marks issues and pull
requests stale after a year without activity and closes them 30 days later. Issues labeled
`waiting-for-feedback` are marked stale after 30 days and closed 7 days later. The `no-stale` label exempts an
issue or pull request.

## AI-assisted contributions

AI tools are welcome. Whatever tools you use, you are the author: you are responsible for every line you
submit and must be able to explain it in review. Tick the AI disclosure box in the PR template if AI tools
helped write the change.

If you work with a coding agent, [AGENTS.md](AGENTS.md) gives it the commands, conventions and boundaries for
this repository, and lists the agent skills in `.agents/skills/` (for example, adding a command end to end).
Agents draft; you review, commit and post. Please do not point an autonomous agent at this repository to open
issues or pull requests on its own.

## Code of conduct, security and license

- Be respectful and constructive in issues, discussions and reviews.
- Report security vulnerabilities as described in the [security policy](SECURITY.md). Do not open a public
  issue for them.
- NRedisStack is released under the [MIT license](LICENSE). By submitting a pull request you agree to
  contribute your change under it.
