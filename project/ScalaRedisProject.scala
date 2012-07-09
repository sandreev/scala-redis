import sbt._
import Keys._
import sbtassembly.Plugin.AssemblyKeys._

object ScalaRedisProject extends Build
{
  lazy val root = Project("RedisClient", file(".")) settings(coreSettings : _*)

  lazy val commonSettings: Seq[Setting[_]] = Seq(
    organization := "net.debasishg",
    version := "2.7.1",
    scalaVersion := "2.9.1",
    scalacOptions ++= Seq("-deprecation", "-unchecked")
  )
 
  lazy val twitterRepo = "twitter.com" at "http://maven.twttr.com/"

  lazy val coreSettings = commonSettings ++ template ++ Seq(
    name := "RedisClient",

    libraryDependencies ++= Seq("commons-pool" % "commons-pool" % "1.5.6",
      "com.github.sgroschupf" % "zkclient" % "0.1",
      "org.slf4j"      % "slf4j-api"     % "1.6.1",
      "org.slf4j"      % "slf4j-log4j12" % "1.6.1"  % "provided",
      "log4j"          % "log4j"         % "1.2.16" % "provided",
      "junit"          % "junit"         % "4.8.1",
      "org.scalatest"  % "scalatest_2.9.1" % "1.6.1" % "test",
      "org.mockito"    % "mockito-all"   % "1.8.4"  % "test",
      "com.twitter"    % "util"          % "1.11.4" % "test" intransitive(),
      "com.twitter"    % "finagle-core"  % "1.9.0" % "test"),

    resolvers := Seq(twitterRepo),
    parallelExecution in Test := false,
    publishTo <<= version { (v: String) => 
      if (v.trim.endsWith("SNAPSHOT")) Some(Resolver.sftp("Jetlore SSH", "qbeast", "/data/repo/snapshots") as("qwhisper"))
      else Some(Resolver.sftp("Jetlore SSH", "qbeast", "/data/repo/releases") as("qwhisper"))
    },
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { repo => false },
    pomExtra := (
      <url>https://github.com/debasishg/scala-redis</url>
      <licenses>
        <license>
          <name>Apache 2.0 License</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:debasishg/scala-redis.git</url>
        <connection>scm:git:git@github.com:debasishg/scala-redis.git</connection>
      </scm>
      <developers>
        <developer>
          <id>debasishg</id>
          <name>Debasish Ghosh</name>
          <url>http://debasishg.blogspot.com</url>
        </developer>
      </developers>),
    unmanagedResources in Compile <+= baseDirectory map { _ / "LICENSE" }
  ) ++ sbtassembly.Plugin.assemblySettings ++ Seq(test in assembly := {})

  lazy val fmpp = TaskKey[Seq[File]]("fmpp")
  lazy val fmppOptions = SettingKey[Seq[String]]("fmpp-options")
  lazy val fmppConfig = config("fmpp") hide

  lazy val template = fmppConfig(Test) ++ fmppConfig(Compile) ++ templateBase
  lazy val templateBase = Seq(
    libraryDependencies += "net.sourceforge.fmpp" % "fmpp" % "0.9.14" % fmppConfig.name,
    ivyConfigurations += fmppConfig,
    fmppOptions := "--ignore-temporary-files" :: Nil,
    fullClasspath in fmppConfig <<= update map { _ select configurationFilter(fmppConfig.name) map Attributed.blank }
  )

  def fmppConfig(c: Configuration): Seq[Setting[_]] = inConfig(c)(Seq(
    sourceGenerators <+= fmpp,
    fmpp <<= fmppTask,
    mappings in packageSrc <<= (managedSources, sourceManaged) map { (srcs, base) => srcs x relativeTo(base) },
    sources <<= managedSources
  ))

  lazy val fmppTask =
    (fullClasspath in fmppConfig, runner in fmpp, unmanagedSources, sourceDirectory, includeFilter in unmanagedSources, sourceManaged, fmppOptions, streams) map { (cp, r, sources, srcRoot, filter, output, args, s) =>
      IO.delete(output)
      val arguments = "-U" +: "all" +: "-S" +: srcRoot.getAbsolutePath +: "-O" +: output.getAbsolutePath +: (args ++ sources.getPaths)
      toError(r.run("fmpp.tools.CommandLine", cp.files, arguments, s.log))
      (output ** filter).get
  }
}
