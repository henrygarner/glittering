(defproject glittering "0.1.2"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [gorillalabs/sparkling "1.2.3"]
                 [t6/from-scala "0.2.1"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :aot [#".*" glittering.serialization sparkling.destructuring]
  :profiles {:default [:base :system :user :provided :spark-1.4.0 :dev]
             :spark-1.4.0 ^{:pom-scope :provided}
             {:dependencies [[org.apache.spark/spark-core_2.10 "1.4.0"]
                             [org.apache.spark/spark-graphx_2.10 "1.4.0"]]}
             :test {:resource-paths ["dev-resources"]
                    :aot [glittering.core
                          glittering.pregel
                          glittering.destructuring
                          glittering.pregel-test
                          glittering.test-utils]}
             :dev {:resource-paths ["data"]
                   :aot [glittering.core
                         glittering.pregel
                         glittering.destructuring
                         glittering.pregel-test
                         glittering.test-utils
                         ]}})
