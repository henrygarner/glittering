(defproject glinting "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [gorillalabs/sparkling "1.2.1"]
                 [t6/from-scala "0.2.1"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :aot [#".*" glinting.serialization sparkling.destructuring]
  :profiles {:default [:base :system :user :provided :spark-1.3.1 :dev]
             :spark-1.3.1 ^{:pom-scope :provided}
             {:dependencies [[org.apache.spark/spark-core_2.11 "1.3.1"]
                             [org.apache.spark/spark-graphx_2.11 "1.3.1"]]}
             :test {:resource-paths ["dev-resources"]
                    :aot [glinting.core
                          glinting.pregel
                          glinting.pregel-test
                          glinting.test-utils]}
             :dev {:resource-paths ["data"]
                   :aot [glinting.core
                         glinting.pregel]}})

;; :exclusions [com.google.code.findbugs/jsr305 commons-net commons-codec commons-io]
;; :aot [#".*" sparkling.serialization sparkling.destructuring glinting.api glinting.examples]
