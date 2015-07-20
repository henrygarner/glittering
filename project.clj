(defproject glittering "0.1.1"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [gorillalabs/sparkling "1.2.1"]
                 [t6/from-scala "0.2.1"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :aot [#".*" glittering.serialization sparkling.destructuring]
  :profiles {:default [:base :system :user :provided :spark-1.3.1 :dev]
             :spark-1.3.1 ^{:pom-scope :provided}
             {:dependencies [[org.apache.spark/spark-core_2.11 "1.3.1"]
                             [org.apache.spark/spark-graphx_2.11 "1.3.1"]]}
             :test {:resource-paths ["dev-resources"]
                    :aot [glittering.core
                          glittering.pregel
                          glittering.pregel-test
                          glittering.test-utils]}
             :dev {:resource-paths ["data"]
                   :aot [glittering.core
                         glittering.pregel]}})
