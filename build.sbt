import scala.collection.immutable.Seq

name := """minimal-cqrs"""
organization := "net.imadz"
logLevel := Level.Info
version := "1.0-SNAPSHOT"
semanticdbEnabled := true
semanticdbVersion := "4.9.7"

val akkaVersion = "2.6.20"
val AkkaManagementVersion = "1.1.4"
val AkkaPersistenceJdbcVersion = "5.1.0"
val AkkaProjectionVersion = "1.2.5"
val ScalikeJdbcVersion = "3.5.0"
//val AlpakkaKafkaVersion = "4.0.0"
val SlickVersion = "3.3.3"
val MongoPluginVersion = "3.0.8"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.13.14"

Compile / scalacOptions ++= Seq(
  "-target:11",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlog-reflective-calls",
  "-Xlint")
Compile / javacOptions ++= Seq("-Xlint:unchecked", "-Xlint:deprecation")

resolvers ++= Seq(
  "Aliyun" at "https://maven.aliyun.com/repository/public/",
  "Huawei" at "https://repo.huaweicloud.com/repository/maven/"
)

//resolvers += "softprops-maven" at "http://dl.bintray.com/content/softprops/maven"
enablePlugins(JavaAppPackaging, DockerPlugin)
dockerBaseImage := "docker.io/library/adoptopenjdk:11-jre-hotspot"
dockerUsername := sys.props.get("docker.username")
dockerRepository := sys.props.get("docker.registry")
ThisBuild / dynverSeparator := "-"

scriptClasspath := Seq("*", "../conf")

libraryDependencies += guice
libraryDependencies += jdbc
libraryDependencies += clusterSharding
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,

  // Akka Management powers Health Checks and Akka Cluster Bootstrapping
  "com.lightbend.akka.management" %% "akka-management" % AkkaManagementVersion,
  "com.lightbend.akka.management" %% "akka-management-cluster-http" % AkkaManagementVersion,
  "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % AkkaManagementVersion,
  "com.lightbend.akka.discovery" %% "akka-discovery-kubernetes-api" % AkkaManagementVersion,
  "com.typesafe.akka" %% "akka-discovery" % akkaVersion,

  // Common Dependencies
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.4.5",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,

  // Akka Persistence
  "com.typesafe.akka" %% "akka-persistence-typed" % akkaVersion,
  "com.github.scullxbones" %% "akka-persistence-mongo-scala" % MongoPluginVersion,
  "com.github.scullxbones" %% "akka-persistence-mongo-rxmongo" % MongoPluginVersion,
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,
  //  "com.lightbend.akka" %% "akka-persistence-jdbc" % AkkaPersistenceJdbcVersion,
  //  "com.typesafe.slick" %% "slick" % SlickVersion,
  //  "com.typesafe.slick" %% "slick-hikaricp" % SlickVersion,
  "com.typesafe.akka" %% "akka-persistence-testkit" % akkaVersion % Test,

  // 4. Querying or projecting data from Akka Persistence
  "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion,
  "com.lightbend.akka" %% "akka-projection-eventsourced" % AkkaProjectionVersion,
  "com.lightbend.akka" %% "akka-projection-jdbc" % AkkaProjectionVersion,
  "org.scalikejdbc" %% "scalikejdbc" % ScalikeJdbcVersion,
  "org.scalikejdbc" %% "scalikejdbc-config" % ScalikeJdbcVersion,
  "org.scalikejdbc" %% "scalikejdbc-play-initializer" % "2.8.0-scalikejdbc-3.5",

  // 5. Excel Loaders
  "org.apache.poi" % "poi" % "5.2.4",
  "org.apache.poi" % "poi-ooxml" % "5.2.4",

  "mysql" % "mysql-connector-java" % "8.0.33",
//  "mysql" % "mysql-connector-java" % "5.1.34",

  // 6. fix jna runtime issue
  "net.java.dev.jna" % "jna" % "5.13.0",
  "net.java.dev.jna" % "jna-platform" % "5.13.0",
  "io.methvin" % "directory-watcher" % "0.17.1",
  "com.lightbend.akka" %% "akka-projection-testkit" % AkkaProjectionVersion % Test
)
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "5.0.0" % Test
// 鎶曞奖浠撳簱 TDD锛圱ask 45 璧凤級锛欻2 鍐呭瓨搴擄紙MySQL 鍏煎妯″紡锛夋壙杞?conf/sql/2.sql 寤鸿〃 + scalikejdbc CRUD 鏂█
libraryDependencies += "com.h2database" % "h2" % "2.2.224" % Test
// 认证（Authentication_Design.md §一）
libraryDependencies += "com.github.jwt-scala" %% "jwt-play-json" % "9.1.2"
libraryDependencies += "com.github.ben-manes.caffeine" % "caffeine" % "3.1.8"
// Neo4j driver 4.4.x：Java 8+ 兼容（本机/脚手架运行时为 Java 11；driver 5.x 需 Java 17，
// 运行时加载即 UnsupportedClassVersionError——GraphSmokeMain 2026-08-24 实测）。JDK 17 采纳后再升级。
libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "4.4.12"
// PostgreSQL + pgvector（M3 向量读模型·kb_tenant_vector）：手写基建 PgVectorStore
// （infrastructure/persistence/vector）直连 JDBC（DriverManager 短连接——检索 QPS 低·嵌入才是瓶颈）。
// 42.7.x 兼容 PG16/pgvector 0.8；仅向量通道使用，MySQL 权威投影链不受影响。
libraryDependencies += "org.postgresql" % "postgresql" % "42.7.4"
libraryDependencies += "nl.gn0s1s" %% "base64" % "0.2.2"
libraryDependencies += "com.github.jwt-scala" %% "jwt-play" % "9.1.2"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "4.11.0"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-protobuf-v3" % akkaVersion,
//  "org.mongodb.scala" %% "mongo-scala-driver" % "4.11.1",
  "com.google.protobuf" % "protobuf-java" % "3.15.6",
  /* 2026-09-28 Mongo6 兼容升级（用户拍板）：RC11 在 killCursors/GetMore 路径发 Mongo 6 已移除的
   * 旧 opcode（服务端实证 "OP_KILL_CURSORS is no longer supported"）→ 40432 Unknown section kind
   * 毒流卡死投影重放。上游 wire 协议适配 GitHub #1372 于 RC16 合入·取最新 RC21。 */
  "org.reactivemongo" %% "reactivemongo" % "1.1.0-noshaded.RC21",
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
)

dependencyOverrides ++= Seq(
  "org.mongodb.scala" %% "mongo-scala-driver" % "4.11.1",
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "org.reactivemongo" %% "reactivemongo" % "1.1.0-noshaded.RC21",
  /* 2026-09-28 Mongo6 兼容升级：akkastream 必须显式 override——rxmongo 3.0.8 传递引入的
   * akkastream 版本（曾为 1.0.10）不会被上一行覆盖（模块名不同），混排＝协议解析错位。 */
  "org.reactivemongo" %% "reactivemongo-akkastream" % "1.1.0-RC21",
  "com.typesafe.play" %% "play-json" % "2.9.3",
  "com.google.guava" % "guava" % "30.1.1-jre",
  "com.typesafe.akka" %% "akka-http-core" % "10.2.7",
  "org.slf4j" % "slf4j-api" % "2.0.4"
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value / "protobuf"
)

addCompilerPlugin("org.scalameta" % "semanticdb-scalac" % "4.9.7" cross CrossVersion.full)
scalacOptions += "-Yrangepos"

testOptions in Test += Tests.Argument("-oDF")
logBuffered in Test := false


