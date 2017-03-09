name := """scala_school_akka_streams"""

version := "1.0"

scalaVersion := "2.12.0"

libraryDependencies ++= Seq (
  "com.typesafe.akka"     %% "akka-stream"          % "2.4.17",
  "com.typesafe.akka"     %% "akka-stream-testkit"  % "2.4.17"  % "test",
  "org.scalatest"         %% "scalatest"            % "3.0.0"   % "test"
)
