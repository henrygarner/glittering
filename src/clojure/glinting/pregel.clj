(ns glinting.pregel
  (:require  [glinting.core :as g]
             [t6.from-scala.core :refer [$] :as $])
  (:import [org.apache.spark.graphx EdgeDirection]))

(defn message-fn [f]
  (fn [ctx]
    (let [incoming {:src-id (.srcId ctx)
                    :src-attr (.srcAttr ctx)
                    :dst-id (.dstId ctx)
                    :dst-attr (.dstAttr ctx)
                    :attr (.attr ctx)}
          outgoing (f incoming)]
      (doseq [[dest message] outgoing]
        (case dest
          :src (.sendToSrc ctx message)
          :dst (.sendToDst ctx message))))))


(defn pregel [{:keys [initial-message max-iterations
                      vertex-fn edge-fn combiner]} graph]
  (let [dir (EdgeDirection/Either)
        g (g/map-vertices (fn [vid attr]
                            (vertex-fn vid attr initial-message))
                          graph)]
    (loop [g g
           messages (g/aggregate-messages edge-fn combiner g)
           i 0]
      (if (and (> (.count messages) 0)
               (< i max-iterations))
        (let [new-verts (.cache (g/inner-join vertex-fn messages (g/vertices g)))
              old-g g
              g (.cache (g/outer-join-vertices (fn [vid old new-opt]
                                                 ($ new-opt getOrElse ($/fn [] old)))
                                               new-verts g))
              old-messages messages
              messages (.cache (g/aggregate-messages edge-fn combiner g))]
          (println "Pregel iteration: " i "messages count: " (.count messages))
          (.unpersist old-messages false)
          (.unpersist new-verts false)
          (.unpersistVertices old-g false)
          (.unpersist (g/edges old-g) false)
          (recur g messages (inc i)))
        g))))
