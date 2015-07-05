# Glittering

A Clojure wrapper around GraphX, Spark's graph processing library.

## Usage

Available from clojars:

```clojure
   [glittering "0.1.0"]
```

See the [Clojure Data Science chapter on Graphs](https://github.com/clojuredatascience/ch8-graphs/blob/master/src/cljds/ch8/examples.clj) for usage examples.

Functions in `glittering.core` are designed to be used with Clojure's double-threading macro.

A Clojure implementation of Pregel is provided. Here's an example which uses the interface for label propagation from the tests.

```clojure
(deftest label-propagation
  (spark/with-context sc (-> (g/conf)
                             (conf/master "local[*]")
                             (conf/app-name "label-propagation-test"))
    (let [vertex-fn (fn [vertex-id attribute message]
                      (if (empty? message)
                        attribute
                        (key (apply max-key val message))))
          edge-fn (p/message-fn
                   (fn [{:keys [src-attr dst-attr]}]
                     {:src {dst-attr 1}
                      :dst {src-attr 1}}))
          edges (spark/parallelize sc (two-cliques 5))
          labels (->> (g/graph-from-edges edges 1)
                      (g/map-vertices (fn [vid attr] vid))
                      (p/pregel {:initial-message {}
                                 :edge-fn edge-fn
                                 :combiner (partial merge-with +)
                                 :vertex-fn vertex-fn
                                 :max-iterations 10})
                      (g/vertices)
                      (spark/collect)
                      (vec)
                      (untuple-all)
                      (group-by second))]
      (testing
          "returns two cliques"
        (is (= 2 (-> labels keys count)))))))
```

## License

Copyright © 2015 Henry Garner

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
