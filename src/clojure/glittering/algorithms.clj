(ns glittering.algorithms)

(defn triangle-count [graph]
  (.triangleCount (.ops graph)))

(defn page-rank [tol reset-prob graph]
  (.pageRank (.ops graph) tol reset-prob)
  #_(.staticPageRank (.ops graph) reset-prob))

(defn connected-components [graph]
  (.connectedComponents (.ops graph)))

(defn strongly-connected-components [iterations graph]
  (.stronglyConnectedComponents (.ops graph) iterations))
