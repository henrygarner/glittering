(ns shimmer.examples
  (:require [shimmer.api :as shimmer]
            [sparkling.api :as spark]
            [sparkling.conf :as conf]
            [t6.from-scala.core :refer [$] :as $])
  (:import [org.apache.spark.graphx.util GraphGenerators$]
           [org.apache.spark.graphx GraphOps]
           [scala.collection Iterator Iterator$]))

(comment (def master "local")
         (def conf {})
         (def env {})

         (def c
           (-> (conf/spark-conf)
               (conf/master master)
               (conf/app-name "svd")
               (conf/set "spark.akka.timeout" "300")
               (conf/set conf)
               (conf/set-executor-env env)))

         (defonce sc
           (spark/spark-context c)))

(defn make-edges [sc]
  (spark/parallelize sc [(shimmer/edge 1 2 3)
                         (shimmer/edge 2 1 5)]))

(defn make-vertices [sc]
  (spark/parallelize-pairs sc [#sparkling/tuple[1 1]
                               #sparkling/tuple[2 2]]))

(defn make-graph []
  (let [v (make-vertices)
        e (make-edges)]
    (shimmer/graph v e)))

(comment (defn generate-graph []
   (let [graph (.logNormalGraph GraphGenerators$/MODULE$ (.sc sc) 5 0 4.0 1.3 -1)]
     (-> graph
         (shimmer/map-edges (fn [e]
                              (double (.attr e))))
         (shimmer/map-vertices (fn [id value]
                                 (if (= id 42)
                                   0 100)))))))

(defn vertex-program [id dist new-dist]
  (println "VERTEX PROG" id dist new-dist)
  new-dist)

#_(if (and (.srcAttr triplet)
           (.attr triplet)
           (< (+ (.srcAttr triplet) (.attr triplet))
              (.dstAttr triplet)))
    ($ Iterator & #sparkling/tuple[(.dstId triplet) (+ (.srcAttr triplet) (.attr triplet))])
    ($ Iterator/empty))

(defn send-message [triplet]
  (println "SEND MESSAGE")
  (println triplet)
  (println (class (.srcId triplet)))
  ($ Iterator & ($/tuple (.dstId triplet) 10.0)))

(defn merge-message [a b]
  (println "MERGE MESSAGE" a)
  a)

(defn test-mr-triplets []
  (spark/with-context sc (-> (conf/spark-conf)
                             (conf/master "local[*]")
                             (conf/app-name "sparkling-test"))
    (let [graph (.logNormalGraph GraphGenerators$/MODULE$ (.sc sc) 5 0 4.0 1.3 -1)]
      (-> graph
          (shimmer/map-edges (fn [e]
                               (double (.attr e))))
          (shimmer/map-vertices (fn [id value]
                                  (if (= id 42)
                                    0 100)))
          (shimmer/map-reduce-triplets send-message merge-message)
          (.first)))))

(defn test-pregel []
  (spark/with-context sc (-> (conf/spark-conf)
                             (conf/master "local[*]")
                             (conf/app-name "sparkling-test"))
    (let [graph (.logNormalGraph GraphGenerators$/MODULE$ (.sc sc) 5 0 4.0 1.3 -1)]
      (-> graph
          (shimmer/map-edges (fn [e]
                               (double (.attr e))))
          (shimmer/map-vertices (fn [id value]
                                  (if (= id 42)
                                    0 100)))
          (shimmer/pregel Double/POSITIVE_INFINITY vertex-program send-message merge-message)
          (.vertices)
          (.first)))))
