(ns p14n.fubsub.data
  (:require [clojure.string :as cs])
  (:import [com.apple.foundationdb FDB ReadTransaction MutationType]
           [com.apple.foundationdb.tuple Tuple Versionstamp]
           [java.util.function Function]))

(defonce fdb (FDB/selectAPIVersion 710))

(defn open-db
  ([cluster-file] (.open fdb cluster-file))
  ([] (.open fdb)))

(defn- ->tuple [ks]
  (Tuple/from (into-array Object ks)))

(defn- pack-tuple [ks]
  (.pack (->tuple ks)))

(defn -set! [tr key-values]
  (run! (fn [[ks v]]
          (let [key (pack-tuple ks)
                value (pack-tuple v)]
            (.set tr key value)))
        key-values))

(defn versionstamp->id-string [vs]
  (if (instance? Versionstamp vs)
    (some->> vs
             (.getTransactionVersion)
             (map #(String/format "%02X" (into-array Object [%])))
             (cs/join "-"))
    (str vs)))

(defn id-string->versionstamp [id]
  (let [id-bytes (some->>
                  (.split #"-" id)
                  (map #(case %
                          "00" 0
                          (-> (BigInteger. % 16)
                              (.toByteArray)
                              (last))))
                  (byte-array)
                  (bytes))]
    (Versionstamp/complete id-bytes 0)))

(defn- append-bytes-to-packed-tuple [packed end-bytes]
  (-> packed
      (concat end-bytes)
      (vec)
      (byte-array)
      (bytes)))

(defn -get [tr keys]
  (->> keys
       (pack-tuple)
       (.get tr)
       (.join)))

(defn -mutate! [tr mutation-type packed-key packed-value]
  (.mutate tr mutation-type packed-key packed-value))

(defn -range [tr begin end limit]
  (.asList (.getRange tr begin end limit)))

(defn -clear
  ([tr keys]
   (->> keys
        (pack-tuple)
        (.clear tr))))

(defn -clear-key-starts-with
  ([tr keys]
   (let [packed (pack-tuple keys)
         begin-packed (append-bytes-to-packed-tuple packed [0x02 0x00])
         end-packed (append-bytes-to-packed-tuple packed [0xFF 0x00])]
     (.clear tr begin-packed end-packed))))

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
   (if db
     (transact! db #(f %))
     (with-open [db (open-db)]
       (transact! db f)))))

(defn ctx-with-tx [ctx tx]
  (assoc ctx :tx tx))

(defn get-value [{:keys [tx db]} keys]
  (println "get-value" keys)
  (some-> (if tx
            (-get tx keys)
            (transact! db #(-get % keys)))
          (Tuple/fromBytes)
          (.getItems)))

(defn put-all [{:keys [tx db]} kvs]
  (println "put-all" kvs)
  (if tx
    (-set! tx kvs)
    (transact! db #(-set! % kvs))))

(defn compare-and-clear [{:keys [tx db]} [k v]]
  (println "compare-and-clear" k v)
  (if tx
    (-mutate! tx MutationType/COMPARE_AND_CLEAR (pack-tuple k) (pack-tuple v))
    (transact! db #(-mutate! tx MutationType/COMPARE_AND_CLEAR (pack-tuple k) (pack-tuple v)))))

(defn set-with-version-key [{:keys [tx db]} [k v]]
  (if tx
    (-mutate! tx MutationType/SET_VERSIONSTAMPED_KEY (.packWithVersionstamp (->tuple k)) (pack-tuple v))
    (transact! db #(-mutate! tx MutationType/SET_VERSIONSTAMPED_KEY (.packWithVersionstamp (->tuple k)) (pack-tuple v)))))

(defn set-with-version-value [{:keys [tx db]} [k v]]
  (if tx
    (-mutate! tx MutationType/SET_VERSIONSTAMPED_VALUE (pack-tuple k) (.packWithVersionstamp (->tuple v)))
    (transact! db #(-mutate! tx MutationType/SET_VERSIONSTAMPED_VALUE (pack-tuple k) (.packWithVersionstamp (->tuple v))))))

;; (defn delete-all [{:keys [tx db]} ks]
;;   (if tx
;;     (-clear tx ks)
;;     (transact! db #(-clear % ks))))

(defn tuple->vector [tuple]
  (some->> tuple
           (.getItems)
           (vec)))

(defn key-value->vector [kv]
  (when kv
    (let [kt (Tuple/fromBytes (.getKey kv))
          vt (Tuple/fromBytes (.getValue kv))]
      [(tuple->vector kt) (tuple->vector vt)])))

(defn get-range-after [{:keys [tx db]} begin limit]
  (let [begin-packed (pack-tuple begin)
        end-packed (-> begin
                       (drop-last)
                       (pack-tuple)
                       (append-bytes-to-packed-tuple [0xFF 0x00]))
        results (some->> (if tx
                           (-range tx begin-packed end-packed limit)
                           (transact! db #(-range % begin-packed end-packed limit)))
                         (.get)
                         (map key-value->vector))
        key-length (count begin)
        first-key-match (= begin (take key-length (ffirst results)))]
    (if first-key-match
      (rest results)
      results)))

(defn get-range-before [{:keys [tx db]} end]
  (let [end-packed (pack-tuple end)
        begin-packed (-> end
                         (drop-last)
                         (pack-tuple)
                         (append-bytes-to-packed-tuple [0x02 0x00]))]
    (some->> (if tx
               (-range tx begin-packed end-packed ReadTransaction/ROW_LIMIT_UNLIMITED)
               (transact! db #(-range % begin-packed end-packed ReadTransaction/ROW_LIMIT_UNLIMITED)))
             (.get)
             (map key-value->vector))))

(defn set-watch [{:keys [tx db]} keys callback]
  (-> (if tx
        (.watch tx (pack-tuple keys))
        (transact! db #(.watch % (pack-tuple keys))))
      (.thenRun callback)))
