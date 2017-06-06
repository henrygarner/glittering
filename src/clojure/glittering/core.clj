(ns glittering.core
  (:refer-clojure :exclude [partition-by])
  (:require [glittering.utils :refer :all]
            [sparkling.conf :as conf]
            [sparkling.scalaInterop :as si]
            [glittering.destructuring :as g-de]
            [t6.from-scala.core :refer [$]])
  (:import [glittering.scalaInterop ScalaFunction2 ScalaFunction3]
           [org.apache.spark SparkConf]
           [org.apache.spark.api.java JavaRDDLike]
           [org.apache.spark.graphx Pregel Edge Graph Graph$ EdgeDirection GraphOps PartitionStrategy$]
           [org.apache.spark.graphx.lib ConnectedComponents]
           [org.apache.spark.storage StorageLevel]
           [scala.reflect ClassTag]
           [sparkling.scalaInterop ScalaFunction1]))

(defn set-glittering-registrator [conf]
  (conf/set conf "spark.kryo.registrator" "glittering.serialization.Registrator"))

(defn conf []
  (-> (SparkConf.)
      (conf/set "spark.serializer" "org.apache.spark.serializer.KryoSerializer")
      (set-glittering-registrator)))

(def edge-directions
  {:in     (EdgeDirection/In)
   :out    (EdgeDirection/Out)
   :either (EdgeDirection/Either)
   :both   (EdgeDirection/Both)})

(defn string->partition [s]
  (.fromString PartitionStrategy$/MODULE$ s))

(def partition-strategies
  {:edge-partition-1d (string->partition "EdgePartition1D")
   :edge-partition-2d (string->partition "EdgePartition2D")
   :canonical-random-vertex-cut (string->partition "CanonicalRandomVertexCut")
   :random-vertex-cut (string->partition "RandomVertexCut")})

(defn graph
  "Create a graph from RDDs containing vertices and edges,"
  [vertices edges]
  ($ Graph (.rdd vertices) (.rdd edges)
     (.apply$default$3 Graph$/MODULE$)
     (.apply$default$4 Graph$/MODULE$)
     (.apply$default$5 Graph$/MODULE$)
     si/OBJECT-CLASS-TAG
     si/OBJECT-CLASS-TAG))

(defn graph-from-edges
  "Create a graph from an RDD of edges and a default node attribute."
  [edges default]
  (.fromEdges Graph$/MODULE$
              (if (instance? JavaRDDLike edges) (.rdd edges) edges)
              default
              (.fromEdges$default$3 Graph$/MODULE$)
              (.fromEdges$default$4 Graph$/MODULE$)
              si/OBJECT-CLASS-TAG
              si/OBJECT-CLASS-TAG))

(defn out-degrees [graph]
  (.outDegrees (.ops graph)))

(defn edge
  ([from to]
   ($ Edge from to nil))
  ([from to attribute]
   ($ Edge from to attribute)))

(defn src-attr [edge]
  (.srcAttr edge))

(defn dst-attr [edge]
  (.dstAttr edge))

(defn src-id [edge]
  (.srcId edge))

(defn dst-id [edge]
  (.dstId edge))

(defn edges [graph]
  (.edges graph))

(defn vertices [graph]
  (.vertices graph))

(defn map-vertices [f graph]
  (.mapVertices graph
                (new ScalaFunction2 f)
                si/OBJECT-CLASS-TAG
                nil))

(defn map-edges [f graph]
  (.mapEdges graph
             (new ScalaFunction1 f)
             si/OBJECT-CLASS-TAG))

(defn map-triplets [f graph]
  (.mapTriplets graph
                (new ScalaFunction1 f)
                si/OBJECT-CLASS-TAG))

(defn inner-join [f other rdd]
  (.innerJoin rdd other
              (new ScalaFunction3 f)
              si/OBJECT-CLASS-TAG
              si/OBJECT-CLASS-TAG))


(defn outer-join-vertices [f vertices graph]
  (let [rdd (if (instance? JavaRDDLike vertices)
              (.rdd vertices) vertices)]
    (.outerJoinVertices graph
                        rdd
                        (new ScalaFunction3
                             (fn [a b c]
                               (f a b (or-nil c))))
                        si/OBJECT-CLASS-TAG
                        si/OBJECT-CLASS-TAG
                        nil)))

(defn aggregate-messages [send merge graph]
  (.aggregateMessages graph
                      (new ScalaFunction1 (g-de/message-fn send))
                      (new ScalaFunction2 merge)
                      (.aggregateMessages$default$3 graph)
                      si/OBJECT-CLASS-TAG))

(defn group-edges [merge graph]
  (.groupEdges graph (new ScalaFunction2 merge)))

(defn collect-edges [direction graph]
  (.collectEdges (.ops graph)
                 (edge-directions direction)))

(defn collect-neighbor-ids [direction graph]
  (.collectNeighborIds (.ops graph) (edge-directions direction)))

(defn partition-by [strategy graph]
  (.partitionBy graph (partition-strategies strategy)))

(defn subgraph [epred vpred graph]
  (.subgraph graph
             (new ScalaFunction1 epred)
             (new ScalaFunction2 vpred)))

(defn connected-components [graph]
  (ConnectedComponents/run graph si/OBJECT-CLASS-TAG si/OBJECT-CLASS-TAG))
