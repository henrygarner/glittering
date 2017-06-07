(ns glittering.pregel
  (:require
    [glittering.core :as g])
  (:import [org.apache.spark.graphx EdgeDirection]))

(defn pregel [{:keys [initial-message max-iterations
                      vertex-fn message-fn combiner
                      direction]
               :or {direction :either
                    max-iterations Long/MAX_VALUE}}
              g]
  (let [dir (g/edge-directions direction)
        g (if initial-message
            (g/map-vertices
             (fn [vid attr] (vertex-fn vid attr initial-message)) g)
            g)]
    (loop [g g
           messages (g/aggregate-messages message-fn combiner g)
           i 0]
      (if (and (> (.count messages) 0)
               (< i max-iterations))
        (let [new-verts (.cache (g/inner-join vertex-fn messages (g/vertices g)))
              old-g g
              g (.cache (g/outer-join-vertices (fn [vid old new-opt]
                                                 (or new-opt old))
                                               new-verts g))
              old-messages messages
              messages (.cache (g/aggregate-messages message-fn combiner g))]
          #_(println "Pregel iteration: " i "messages count: " (.count messages))
          (.count messages) ;; We must realise the messages
          (.unpersist old-messages false)
          (.unpersist new-verts false)
          (.unpersistVertices old-g false)
          (.unpersist (g/edges old-g) false)
          (recur g messages (inc i)))
        g))))
