name := "hadoop-test"

version := "1.0"

autoScalaLibrary := false

libraryDependencies ++= Seq(
  "com.algorithmia" % "algorithmia-client" % "1.0.4",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.7.1",
  "org.apache.hadoop" % "hadoop-client" % "2.7.1"
)

mainClass := Some("algorithmia.hadoop.WordCount")

fork in (run) := true