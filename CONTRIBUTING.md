# Contributing

## Introduction

We appreciate your interest in contributing to NRedisStack.
Community contributions mean a lot to us.

## Contributions we need

You may already know how you'd like to contribute, whether it's a fix for a bug you
encountered, or a new feature your team wants to use.

If you don't know where to start, consider improving
documentation, triaging bugs, or writing tutorials. These are all examples of
helpful contributions that mean less work for you.

## Your First Contribution

Unsure where to begin contributing? You can start by looking through
[help-wanted
issues](https://github.com/redis/NRedisStack/labels/help-wanted).

Never contributed to open source before? Here are a couple of friendly
tutorials:

-   <http://makeapullrequest.com/>
-   <http://www.firsttimersonly.com/>

## Getting Started

Here's how to get started with your code contribution:

1.  Create your own fork of NRedisStack
2.  Do the changes in your fork
3.  Write your tests

4.  Use the `docker run -p 6379:6379 -it redis/redis-stack-server:edge` as your local environment for running the functional tests.
5.  Make sure your tests pass using `dotnet test'
6.  Push your changes to GitHub
7.  Open a pull request

## Testing

Call `dotnet test` to run all tests

to test specific test you can use `--filter` flag:
```bash
dotnet test --filter <YourTestName>
```

If you want to run your tests against a specific host and port, you can do it thus:
```bash
dotnet test --environment="REDIS=<redisServer:port>"
```
e.g:
```bash
dotnet test --environment="REDIS=172.17.0.1:6379"
```
## How to Report a Bug

### Security Vulnerabilities

**NOTE**: If you find a security vulnerability, do NOT open an issue.
Email [Redis Open Source (<oss@redis.com>)](mailto:oss@redis.com) instead.

In order to determine whether you are dealing with a security issue, ask
yourself these two questions:

-   Can I access something that's not mine, or something I shouldn't
    have access to?
-   Can I disable something for other people?

If the answer to either of those two questions are *yes*, then you're
probably dealing with a security issue. Note that even if you answer
*no*  to both questions, you may still be dealing with a security
issue, so if you're unsure, just email [us](mailto:oss@redis.com).

### Everything Else

When filing an issue, make sure to answer these five questions:

1.  What version of NRedisStack are you using?
2.  What version of redis are you using?
3.  What did you do?
4.  What did you expect to see?
5.  What did you see instead?

## Suggest a feature or enhancement

If you'd like to contribute a new feature, make sure you check our
issue list to see if someone has already proposed it. Work may already
be underway on the feature you want or we may have rejected a
feature like it already.

If you don't see anything, open a new issue that describes the feature
you would like and how it should work.

## Code review process

The core team regularly looks at pull requests. We will provide
feedback as as soon as possible. After receiving our feedback, please respond
within two weeks. After that time, we may close your PR if it isn't
showing any activity.
