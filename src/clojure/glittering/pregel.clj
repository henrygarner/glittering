(ns glittering.pregel
  (:require  [glittering.core :as g]
             [t6.from-scala.core :refer [$] :as $])
  (:import [org.apache.spark.graphx EdgeDirection]))

(defn pregel
  ([f graph]
   (pregel f {} graph))
  ([f {:keys [max-iterations direction]
       :or {direction :either
            max-iterations Long/MAX_VALUE}}
    g]
   (let [dir (g/edge-directions direction)
         init (f)
         g (g/map-vertices #(f %1 %2 init) g)]
     (loop [g g
            messages (g/aggregate-messages #(f %1) #(f %1 %2) g)
            i 0]
       (if (and (> (.count messages) 0)
                (< i max-iterations))
         (let [new-verts (.cache (g/inner-join #(f %1 %2 %3) messages (g/vertices g)))
               old-g g
               g (.cache (g/outer-join-vertices (fn [vid old new-opt]
                                                  (or new-opt old))
                                                new-verts g))
               old-messages messages
               messages (.cache (g/aggregate-messages #(f %1) #(f %1 %2) g))]
           #_(println "Pregel iteration: " i "messages count: " (.count messages))
           (.count messages) ;; We must realise the messages
           (.unpersist old-messages false)
           (.unpersist new-verts false)
           (.unpersistVertices old-g false)
           (.unpersist (g/edges old-g) false)
           (recur g messages (inc i)))
         g)))))
