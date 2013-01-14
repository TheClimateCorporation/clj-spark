(defproject clj-spark/clj-spark "0.1.0-SNAPSHOT"

  :min-lein-version "2.0.0"

  :license {:name "Eclipse Public License" :url "http://www.eclipse.org/legal/epl-v10.html"}
  :description "Clojure API wrapper on the Spark project (http://spark-project.org/)"
  :url "https://github.com/TheClimateCorporation/clj-spark"

  :jvm-opts ["-Dlog4j.configuration=file:log4j.properties"]

  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.clojure/tools.cli "0.2.1"]
                 [org.clojars.mlimotte/serializable-fn "0.0.3"]]

  ; Include te Spark dep for compile and run, but not for uberjar
  :profiles {
      :dev {:dependencies [[org.spark-project/spark-core_2.9.2 "0.6.1"]]}}

  :aot [clj-spark.spark.functions
        clj-spark.api
        clj-spark.util
        weatherbill.query
        clj-spark.examples.query]

  :main clj-spark.examples.query)
