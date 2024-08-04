(ns test-util)

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
         (drop-while #(>= 0 (compare (nth (first %) idx) last-key)))
         (take limit)
         (vec))))

(def db (atom {}))

(defn get-value [keys]
  (get @db keys))

(defn get-range-after [keys limit]
  (>key-range keys @db limit))

(defn get-range [keys]
  (key-range keys @db))

(defn put-all [kvs]
  (swap! db #(apply merge % kvs)))