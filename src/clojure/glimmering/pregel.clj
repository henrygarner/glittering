(ns glimmering.pregel
  (:require  [glimmering.core :as g]
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
      (doseq [message (:src outgoing)]
        (.sendToSrc ctx message))
      (doseq [message (:dst outgoing)]
        (.sendToDst ctx message)))))

(defn pregel [graph init max-iterations vf sf mf]
  (let [dir (EdgeDirection/Either)
        g (g/map-vertices graph (fn [vid attr]
                                  (vf vid attr init)))]
    (loop [g g
           messages (g/aggregate-messages g sf mf)
           i 0]
      (if (and (> (.count messages) 0)
               (< i max-iterations))
        (let [new-verts (.cache (g/inner-join (g/vertices g) messages vf))
              old-g g
              g (.cache (g/outer-join-vertices g new-verts
                                               (fn [vid old new-opt]
                                                 ($ new-opt getOrElse ($/fn [] old)))))
              old-messages messages
              messages (.cache (g/aggregate-messages g sf mf))]
          (println "Pregel iteration: " i "messages count: " (.count messages))
          (.unpersist old-messages false)
          (.unpersist new-verts false)
          (.unpersistVertices old-g false)
          (.unpersist (g/edges old-g) false)
          (recur g messages (inc i)))
        g))))
