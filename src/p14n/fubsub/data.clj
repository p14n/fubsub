(ns p14n.fubsub.data
  (:import [com.apple.foundationdb FDB ReadTransaction]
           [com.apple.foundationdb.tuple Tuple]
           [java.util.function Function]))

;; :get-range-after tu/get-range-after
;; :get-value tu/get-value
;; :get-range-before tu/get-range-before
;; :put-all tu/put-all
;; :delete tu/delete-all

(defonce fdb (FDB/selectAPIVersion 710))

(defn open-db
  ([cluster-file] (.open fdb cluster-file))
  ([] (.open fdb)))

(defn- pack-tuple [ks]
  (.pack (Tuple/from (into-array Object ks))))

(defn -set! [tr key-values]
  (run! (fn [[ks v]]
          (let [key (pack-tuple ks)
                value (pack-tuple [v])]
            (.set tr key value)))
        key-values))

(defn -get [tr keys]
  (->> keys
       (pack-tuple)
       (.get tr)
       (.join)))

(defn -range [tr begin end limit]
  (let [b (pack-tuple begin)
        e (pack-tuple end)]
    (.join (.getRange tr b e limit))))

(defn -clear [tr keys]
  (->> keys
       (pack-tuple)
       (.clear tr)))

(defn bytes-to-string [bytes]
  (-> bytes
      (Tuple/fromBytes)
      (.getString 0)))

(defn transact!
  [db f]
  (.run db
        (reify
          Function
          (apply [_ tr]
            (f tr)))))

(defn with-transaction
  ([{:keys [db]} f]
   (transact! db #(f %))))

(defn ctx-with-tx [ctx tx]
  (assoc ctx :tx tx))

(defn get-value [{:keys [tx db]} keys]
  (if tx
    (-get tx keys)
    (transact! db #(-get % keys))))

(defn put-all [{:keys [tx db]} kvs]
  (if tx
    (-set! tx kvs)
    (transact! db #(-set! % kvs))))

(defn delete-all [{:keys [tx db]} ks]
  (if tx
    (-clear tx ks)
    (transact! db #(-clear % ks))))

(defn get-range-after [{:keys [tx db]} begin limit]
  (let [end (conj (vec (drop 1 begin)) (byte 0xFF))]
    (if tx
      (-range tx begin end limit)
      (transact! db #(-range % begin end limit)))))

(defn get-range-before [{:keys [tx db]} end]
  (let [begin (conj (vec (drop 1 end)) "")]
    (if tx
      (-range tx begin end ReadTransaction/ROW_LIMIT_UNLIMITED)
      (transact! db #(-range % begin end ReadTransaction/ROW_LIMIT_UNLIMITED)))))
