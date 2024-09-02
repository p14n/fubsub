(ns test-util
  (:require [clojure.string :as string]))

(defn key-range [key m]
  (let [c (count key)]
    (->> m
         (filterv (fn [[k _]]
                    (= key (take c k)))))))

(defn >key-range
  [key m limit]
  (let [last-key (last key) idx (dec (count key))
        kvs (key-range (drop-last key) m)
        ;_ (println ">>>" last-key idx kvs)
        ]
    (->> kvs
         (drop-while #(>= 0 (compare (nth (first %) idx) last-key)))
         (take limit)
         (vec))))

(defn <key-range [key m]
  (let [last-key (last key)
        idx (dec (count key))
        kvs (key-range (drop-last key) m)
        ;_ (println "<<<" last-key idx kvs)
        ]
    (->> kvs
         (take-while #(> 0 (compare (nth (first %) idx) last-key)))
         (vec))))

(def db (atom {}))

(defn get-value [_ keys]
  ;(println @db keys (->> keys (map type) (map str) (clojure.string/join " ")))
  (get @db keys))

(defn get-range-after [_ keys limit]
  (>key-range keys @db limit))

(defn get-range-before [_ keys]
  (<key-range keys @db))

(defn put-all [_ kvs]
  (println "put-all" kvs)
  (swap! db #(->> kvs
                  (concat (vec %))
                  (sort-by first)
                  (into (sorted-map)))))

(defn compare-and-clear [_ [k v]]
  (println "compare-and-clear" k v)
  (swap! db #(if (= v (get % k))
               (dissoc % k)
               (throw (ex-info "compare-and-clear failed" {:k k :v v :value (get % k) :db @db})))))


