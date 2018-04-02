import scalariform.formatter.preferences._

lazy val Versions = new {
  val akka = "2.5.11"
  val scalaTest = "3.0.4"
  val scalaVersion = "2.12.4"
}

lazy val Libraries = new {
  val akka                    = "com.typesafe.akka" %% "akka-actor"                 % Versions.akka
  val akkaStreams             = "com.typesafe.akka" %% "akka-stream"                % Versions.akka
  val akkaClusterSharding     = "com.typesafe.akka" %% "akka-cluster-sharding"      % Versions.akka
  val akkaTestKit             = "com.typesafe.akka" %% "akka-testkit"               % Versions.akka        % "test"
  val scalaTest               = "org.scalatest"     %% "scalatest"                  % Versions.scalaTest   % "test"
}

scalaVersion in ThisBuild := Versions.scalaVersion

organization in ThisBuild := "au.id.fsat"
organizationName in ThisBuild := "Felix Satyaputra"
startYear in ThisBuild := Some(2018)
licenses in ThisBuild += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt"))

scalariformPreferences in ThisBuild := scalariformPreferences.value
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(AllowParamGroupsOnNewlines, true)

enablePlugins(AutomateHeaderPlugin)

lazy val susan = project
  .in(file("."))
  .aggregate(
    calvin
  )

lazy val calvin = project
  .in(file("calvin"))
  .settings(Seq(
    libraryDependencies ++= Seq(
      Libraries.akka,
      Libraries.akkaStreams,
      Libraries.akkaClusterSharding,
      Libraries.scalaTest,
      Libraries.akkaTestKit
    ),
    parallelExecution in Test := false
  ))
