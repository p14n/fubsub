(ns test-util
  (:require [clojure.test :refer [deftest is testing]]))

(defn key-range [key m]
  (let [c (count key)]
    (->> m
         (filterv (fn [[k _]]
                    (= key (take c k)))))))

(defn >key-range [key m limit]
  (let [last-key (last key)
        idx (dec (count key))
        kvs (key-range (drop-last key) m)]
    (->> kvs
         (drop-while #(> 0 (compare (nth (first %) idx) last-key)))
         (take limit)
         (vec))))

(deftest testing-key-range
  (testing "key-range selects all subkeys of the key"
    (is (= (key-range [1 2] {[1 2 3] 1,
                             [1 2] 2,
                             [3 4] 3})
           [[[1 2 3] 1]
            [[1 2] 2]])))
  (testing ">key-range selects all keys greater than the key"
    (is (= (>key-range [1 2 3] {[1 2 2] 0,
                                [1 2 3] 1,
                                [1 2 4] 2,
                                [1 2 5] 3
                                [1] 4
                                [1 3 4] 5})
           [[[1 2 4] 2]
            [[1 2 5] 3]]))))

(def db (atom {}))

(defn get-value [keys]
  (get @db keys))

(defn get-range-after [keys limit]
  (>key-range keys @db limit))

(defn get-range [keys]
  (key-range keys @db))

(defn put-all [kvs]
  (swap! db #(apply merge % kvs)))