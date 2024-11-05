# How to Contribute

We'd love to get patches from you!

## Building Stitch

Stitch is built using [sbt][sbt]. When building please use the included
[`./sbt`](https://github.com/twitter/stitch/blob/develop/sbt) script which
provides a thin wrapper over [sbt][sbt] and correctly sets memory and other
settings.

If you have any questions or run into any problems, please create an issue here on GitHub.

## Workflow

We follow the [GitHub Flow Workflow](https://guides.github.com/introduction/flow/)

1.  Fork stitch
1.  Check out the `develop` branch
1.  Create a feature branch
1.  Write code and tests for your change
1.  From your branch, make a pull request against `twitter/stitch/develop`
1.  Work with repo maintainers to get your change reviewed
1.  Wait for your change to be pulled into `twitter/stitch/develop`
1.  Delete your feature branch

## Checklist

There are a number of things we like to see in pull requests. Depending
on the scope of your change, there may not be many to take care of, but
please scan this list and see which apply. It's okay if something is missed;
the maintainers will help out during code review.

1. Include [tests](CONTRIBUTING.md#testing).
1. Update the [changelog][changelog] for new features, API breakages, runtime behavior changes, deprecations, and bug fixes.
1. All public APIs should have [Scaladoc][scaladoc].
1. When adding a constructor to an existing class or arguments to an existing
   method, in order to preserve backwards compatibility for Java users, avoid
   Scala's default arguments. Instead use explicit forwarding methods.
1. The second argument of an `@deprecated` annotation should be the current
   date, in `YYYY-MM-DD` form.

## Testing

We've standardized on using the [ScalaTest testing framework][scalatest].
Because ScalaTest has such a big surface area, we use a restricted subset of it
in our tests to keep them easy to read.  We've chosen the `Matchers` API, and we
use the [`FunSuite` mixin][funsuite]. Please extend our [Test suite][test-suite]
to get these defaults.

Note that while you will see a [Travis CI][travis-ci] status message in your
pull request, all changes will also be tested internally at X before being
merged.

### Property-based testing

When appropriate, use [ScalaCheck][scalacheck] to write property-based
tests for your code. This will often produce more thorough and effective
inputs for your tests. We use ScalaTest's
[GeneratorDrivenPropertyChecks][gendrivenprop] as the entry point for
writing these tests.

## Compatibility

We try to keep public APIs stable for the obvious reasons. Often,
compatibility can be kept by adding a forwarding method. Note that we
avoid adding default arguments because this is not a compatible change
for our Java users.  However, when the benefits outweigh the costs, we
are willing to break APIs. The break should be noted in the Breaking
API Changes section of the [changelog][changelog]. Note that changes to
non-public APIs will not be called out in the [changelog][changelog].

## Java

While the project is written in Scala, its public APIs should be usable from
Java. This occasionally works out naturally from the Scala interop, but more
often than not, if care is not taken Java users will have rough corners
(e.g. `SomeCompanion$.MODULE$.someMethod()` or a symbolic operator).
We take a variety of approaches to minimize this.

1. Add a "compilation" unit test, written in Java, that verifies the APIs are
   usable from Java.
1. If there is anything gnarly, we add Java adapters either by adding
   a non-symbolic method name or by adding a class that does forwarding.
1. Prefer `abstract` classes over `traits` as they are easier for Java
   developers to extend.

## Style

We generally follow the [Scala Style Guide][scala-style-guide]. When in doubt,
look around the codebase and see how it's done elsewhere.

## Issues

When creating an issue please try to adhere to the following format:

    module-name: One line summary of the issue (less than 72 characters)

    ### Expected behavior

    As concisely as possible, describe the expected behavior.

    ### Actual behavior

    As concisely as possible, describe the observed behavior.

    ### Steps to reproduce the behavior

    List all relevant steps to reproduce the observed behavior.

## Pull Requests

Comments should be formatted to a width no greater than 80 columns.

Files should be exempt of trailing spaces.

We adhere to a specific format for commit messages. Please write your commit
messages along these guidelines. Please keep the line width no greater than 80
columns (You can use `fmt -n -p -w 80` to accomplish this).

    module-name: One line description of your change (less than 72 characters)

    Problem

    Explain the context and why you're making that change.  What is the problem
    you're trying to solve? In some cases there is not a problem and this can be
    thought of being the motivation for your change.

    Solution

    Describe the modifications you've done.

    Result

    What will change as a result of your pull request? Note that sometimes this
    section is unnecessary because it is self-explanatory based on the solution.

Some important notes regarding the summary line:

* Describe what was done; not the result
* Use the active voice
* Use the present tense
* Capitalize properly
* Do not end in a period — this is a title/subject
* Prefix the subject with its scope (stitch-http, stitch-jackson, stitch-*)

## Code Review

The Stitch repository on GitHub is kept in sync with an internal repository at
X. For the most part this process should be transparent to Stitch users,
but it does have some implications for how pull requests are merged into the
codebase.

When you submit a pull request on GitHub, it will be reviewed by the Stitch
community (both inside and outside of X), and once the changes are
approved, your commits will be brought into X's internal system for
additional testing. Once the changes are merged internally, they will be pushed
back to GitHub with the next sync.

This process means that the pull request will not be merged in the usual way.
Instead a member of the Stitch team will post a message in the pull request
thread when your changes have made their way back to GitHub, and the pull
request will be closed (see [this pull request in Finatra][pull-example] for an example).
The changes in the pull request will be collapsed into a single commit, but the
authorship metadata will be preserved.

## Documentation

We also welcome improvements to the Stitch documentation or to the existing
Scaladocs. Please file an [issue](https://github.com/twitter/stitch/issues).

[changelog]: CHANGELOG.md
[master-branch]: https://github.com/twitter/stitch/tree/master
[develop-branch]: https://github.com/twitter/stitch/tree/develop
[pull-example]: https://github.com/twitter/finagle/pull/267
[funsuite]: https://doc.scalatest.org/2.2.1/#org.scalatest.FunSuite
[scalatest]: https://www.scalatest.org/
[scala-style-guide]: https://docs.scala-lang.org/style/index.html
[sbt]: https://www.scala-sbt.org/
[travis-ci]: https://travis-ci.org/twitter/stitch
[test-suite]: https://github.com/twitter/stitch/blob/develop/stitch/stitch-core/src/test/scala/com/twitter/stitch/StitchTestSuite.scala
[scaladoc]: https://docs.scala-lang.org/style/scaladoc.html
[scalacheck]: https://www.scalacheck.org/
[gendrivenprop]: https://www.scalatest.org/user_guide/generator_driven_property_checks

### License

By contributing your code, you agree to license your contribution under the
terms of the Apache 2.0 License: https://github.com/twitter/stitch/blob/master/LICENSE
