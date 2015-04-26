(defproject glimmering "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [gorillalabs/sparkling "1.2.1"]
                 [t6/from-scala "0.2.1"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :aot :all 
  :profiles {:default [:base :system :user :provided :spark-1.3.1 :dev]
             :spark-1.3.1 ^{:pom-scope :provided}
             {:dependencies [[org.apache.spark/spark-core_2.11 "1.3.1"]
                             [org.apache.spark/spark-graphx_2.11 "1.3.1"]]}
             :dev {:resource-paths ["data"]}})

;; :exclusions [com.google.code.findbugs/jsr305 commons-net commons-codec commons-io]
;; :aot [#".*" sparkling.serialization sparkling.destructuring glimmering.api glimmering.examples]
