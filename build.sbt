import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.MergeStrategy

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file(".")).
  settings(
    name := "spark-cloud-scala",
    version := "0.1.0",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.2" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided",

      // This is CRITICAL: matches your Maven project
      "org.apache.spark" %% "spark-mllib-local" % "3.3.2",

      // Breeze exactly as in Maven
      "org.scalanlp" %% "breeze" % "1.2",
      "org.scalanlp" %% "breeze-natives" % "1.2"
    ),

    // No shading needed since Spark 3.3.2 is compatible
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @_*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
