import scala.language.reflectiveCalls

// All Twitter library releases are date versioned as YY.MM.patch
val releaseVersion = "19.5.0-SNAPSHOT"
val twitterLibVersion = "22.7.0" // util version

lazy val buildSettings = Seq(
  version := releaseVersion,
  scalaVersion := "2.12.8",
  crossScalaVersions := Seq("2.12.8"),
  scalaModuleInfo := scalaModuleInfo.value.map(_.withOverrideScalaVersion(true)),
  fork in Test := true,
  javaOptions in Test ++= travisTestJavaOptions
)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false,
  // sbt-pgp's publishSigned task needs this defined even though it is not publishing.
  publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo")))
)

def travisTestJavaOptions: Seq[String] = {
  // When building on travis-ci, we want to suppress logging to error level only.
  val travisBuild = sys.env.getOrElse("TRAVIS", "false").toBoolean
  if (travisBuild) {
    Seq(
      "-DSKIP_FLAKY=true",
      "-Dsbt.log.noformat=true",
      // Needed to avoid cryptic EOFException crashes in forked tests
      // in Travis with `sudo: false`.
      // See https://github.com/sbt/sbt/issues/653
      // and https://github.com/travis-ci/travis-ci/issues/3775
      "-Xmx3G"
    )
  } else {
    Seq("-DSKIP_FLAKY=true")
  }
}

lazy val versions = new {
  // When building on travis-ci, querying for the branch name via git commands
  // will return "HEAD", because travis-ci checks out a specific sha.
  val travisBranch = sys.env.getOrElse("TRAVIS_BRANCH", "")

  // All Twitter library releases are date versioned as YY.MM.patch
  val twLibVersion = twitterLibVersion

  val junit = "4.12"
  val mockito = "1.9.5"
  val scalaCheck = "1.13.4"
  val scalaTest = "3.0.5"
  val caffeine = "2.9.3"
  val scalaCheckPlus = "3.1.2.0"
  val jmh = "1.21"
}

lazy val scalaCompilerOptions = scalacOptions ++= Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xlint",
  "-Ywarn-unused-import"
)

lazy val baseSettings = Seq(
  libraryDependencies ++= Seq(
    "com.twitter" %% "util-core" % versions.twLibVersion,
    "com.twitter" %% "util-mock" % versions.twLibVersion % Test,
    "org.mockito" % "mockito-core" % versions.mockito % Test,
    "org.scalacheck" %% "scalacheck" % versions.scalaCheck % Test,
    "org.scalatest" %% "scalatest" % versions.scalaTest % Test,
    "com.github.ben-manes.caffeine" % "caffeine" % versions.caffeine,
    "org.scalatestplus" %% "scalacheck-1-14" % versions.scalaCheckPlus % Test,
    "junit" % "junit" % versions.junit % Test,
    "org.openjdk.jmh" % "jmh-core" % versions.jmh % Test
  ),
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  ),
  organization := "com.twitter",
  scalaCompilerOptions,
  javacOptions in (Compile, compile) ++= Seq(
    "-source",
    "1.8",
    "-target",
    "1.8",
    "-Xlint:unchecked"),
  javacOptions in doc ++= Seq("-source", "1.8"),
  // -a: print stack traces for failing asserts
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v"),
  // broken in 2.12 due to: https://issues.scala-lang.org/browse/SI-10134
  scalacOptions in (Compile, doc) ++= {
    if (scalaVersion.value.startsWith("2.12")) Seq("-no-java-comments")
    else Nil
  }
)

lazy val publishSettings = Seq(
  publishMavenStyle := true,
  publishArtifact in Compile := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  licenses := Seq("Apache 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")),
  homepage := Some(url("https://github.com/twitter/stitch")),
  autoAPIMappings := true,
  apiURL := Some(url("https://twitter.github.io/stitch/scaladocs/")),
  excludeFilter in (Compile, managedSources) := HiddenFileFilter || "BUILD",
  excludeFilter in (Compile, unmanagedSources) := HiddenFileFilter || "BUILD",
  excludeFilter in (Compile, managedResources) := HiddenFileFilter || "BUILD",
  excludeFilter in (Compile, unmanagedResources) := HiddenFileFilter || "BUILD",
  pomExtra :=
    <scm>
      <url>git://github.com/twitter/stitch.git</url>
      <connection>scm:git://github.com/twitter/stitch.git</connection>
    </scm>
    <developers>
      <developer>
        <id>twitter</id>
        <name>Twitter Inc.</name>
        <url>https://www.twitter.com/</url>
      </developer>
    </developers>,
  pomPostProcess := { (node: scala.xml.Node) =>
    val rule = new scala.xml.transform.RewriteRule {
      override def transform(n: scala.xml.Node): scala.xml.NodeSeq =
        n.nameToString(new StringBuilder()).toString() match {
          case "dependency" if (n \ "groupId").text.trim == "org.scoverage" => Nil
          case _ => n
        }
    }

    new scala.xml.transform.RuleTransformer(rule).transform(node).head
  },
  resourceGenerators in Compile += Def.task {
    val dir = (resourceManaged in Compile).value
    val file = dir / "com" / "twitter" / name.value / "build.properties"
    val buildRev = scala.sys.process.Process("git" :: "rev-parse" :: "HEAD" :: Nil).!!.trim
    val buildName = new java.text.SimpleDateFormat("yyyyMMdd-HHmmss").format(new java.util.Date)
    val contents =
      s"name=${name.value}\nversion=${version.value}\nbuild_revision=$buildRev\nbuild_name=$buildName"
    IO.write(file, contents)
    Seq(file)
  }.taskValue,
  assemblyMergeStrategy in assembly := {
    case "BUILD" => MergeStrategy.discard
    case other => MergeStrategy.defaultMergeStrategy(other)
  }
)

lazy val core = project
  .in(file("."))
  .enablePlugins(ScalaUnidocPlugin)
  .settings(baseSettings)
  .settings(buildSettings)
  .settings(noPublishSettings)
  .settings(
    moduleName := "stitch-core",
    unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject,
    excludeFilter in Test in unmanagedResources := "BUILD",
    publishArtifact in Test := true
  )

lazy val site = (project in file("docs"))
  .enablePlugins(SphinxPlugin)
  .settings(
    Seq(sourceDirectory in Sphinx := baseDirectory.value) ++
      baseSettings ++ buildSettings ++ Seq(
      scalacOptions in doc ++= Seq("-doc-title", "Stitch", "-doc-version", version.value),
      includeFilter in Sphinx := ("*.html" | "*.png" | "*.svg" | "*.js" | "*.css" | "*.gif" | "*.txt")
    ))
