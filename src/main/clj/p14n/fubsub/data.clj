(ns p14n.fubsub.data
  (:require [clojure.string :as cs]
            [p14n.fubsub.util :as u]
            [p14n.fubsub.logging :as log])
  (:import [com.apple.foundationdb FDB ReadTransaction MutationType]
           [com.apple.foundationdb.tuple Tuple Versionstamp]
           [java.util.function Function]))

(defonce fdb (FDB/selectAPIVersion 710))

(defn open-db
  ([cluster-file] (.open fdb cluster-file))
  ([] (.open fdb)))

(defn- ->tuple
  ([ks]
   (->tuple ks nil))
  ([ks subspace]
   (Tuple/from (into-array Object (concat subspace ks)))))

(defn- pack-tuple
  ([v]
   (pack-tuple v nil))
  ([ks subspace]
   (.pack (->tuple ks subspace))))

(defn -set! [tr key-values subspace]
  (run! (fn [[ks v]]
          (let [key (pack-tuple ks subspace)
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

(defn -get [tr keys subspace]
  (->> (pack-tuple keys subspace)
       (.get tr)
       (.join)))

(defn -mutate! [tr mutation-type packed-key packed-value]
  (.mutate tr mutation-type packed-key packed-value))

(defn -range [tr begin end limit]
  (.asList (.getRange tr begin end limit)))

(defn -clear
  ([tr keys subspace]
   (->> (pack-tuple keys subspace)
        (.clear tr))))

(defn -clear-key-starts-with
  ([tr keys subspace]
   (let [packed (pack-tuple keys subspace)
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

(defn get-value [{:keys [tx db subspace logger]} keys]
  (log/debug logger :data/get-value keys)
  (some-> (if tx
            (-get tx keys subspace)
            (transact! db #(-get % keys subspace)))
          (Tuple/fromBytes)
          (.getItems)))

(defn put-all [{:keys [tx db subspace logger]} kvs]
  (log/debug logger :data/put-all kvs)
  (if tx
    (-set! tx kvs subspace)
    (transact! db #(-set! % kvs subspace))))

(defn compare-and-clear [{:keys [tx db subspace logger]} [k v]]
  (log/debug logger :data/compare-and-clear [k v])

  (if tx
    (-mutate! tx MutationType/COMPARE_AND_CLEAR (pack-tuple k subspace) (pack-tuple v))
    (transact! db #(-mutate! tx MutationType/COMPARE_AND_CLEAR (pack-tuple k subspace) (pack-tuple v)))))

(defn set-with-version-key [{:keys [tx db subspace]} [k v]]
  (if tx
    (-mutate! tx MutationType/SET_VERSIONSTAMPED_KEY (.packWithVersionstamp (->tuple k subspace)) (pack-tuple v))
    (transact! db #(-mutate! tx MutationType/SET_VERSIONSTAMPED_KEY (.packWithVersionstamp (->tuple k subspace)) (pack-tuple v)))))

(defn set-with-version-value [{:keys [tx db subspace]} [k v]]
  (if tx
    (-mutate! tx MutationType/SET_VERSIONSTAMPED_VALUE (pack-tuple k subspace) (.packWithVersionstamp (->tuple v)))
    (transact! db #(-mutate! tx MutationType/SET_VERSIONSTAMPED_VALUE (pack-tuple k subspace) (.packWithVersionstamp (->tuple v))))))

(defn tuple->vector [tuple]
  (some->> tuple
           (.getItems)
           (vec)))

(defn key-value->vector [kv]
  (when kv
    (let [kt (Tuple/fromBytes (.getKey kv))
          vt (Tuple/fromBytes (.getValue kv))]
      [(tuple->vector kt) (tuple->vector vt)])))

(defn get-range-after [{:keys [tx db subspace logger] :as ctx} begin limit]
  (let [begin-packed (pack-tuple begin subspace)
        _ (log/debug logger :data/get-range-after (->tuple begin subspace))
        end-packed (-> begin
                       (drop-last)
                       (pack-tuple subspace)
                       (append-bytes-to-packed-tuple [0xFF 0x00]))
        results (some->> (if tx
                           (-range tx begin-packed end-packed limit)
                           (transact! db #(-range % begin-packed end-packed limit)))
                         (.get)
                         (map key-value->vector))
        key-length (count begin)
        first-key-match (= begin (take key-length (u/key-without-subspace ctx (ffirst results))))]
    (log/debug logger :data/get-range-after (if first-key-match
                                              (rest results)
                                              results))
    (if first-key-match
      (rest results)
      results)))

(defn get-range-before [{:keys [tx db subspace]} end]
  (let [end-packed (pack-tuple end subspace)
        begin-packed (-> end
                         (drop-last)
                         (pack-tuple subspace)
                         (append-bytes-to-packed-tuple [0x02 0x00]))]
    (some->> (if tx
               (-range tx begin-packed end-packed ReadTransaction/ROW_LIMIT_UNLIMITED)
               (transact! db #(-range % begin-packed end-packed ReadTransaction/ROW_LIMIT_UNLIMITED)))
             (.get)
             (map key-value->vector))))

(defn set-watch [{:keys [tx db subspace]} keys callback]
  (-> (if tx
        (.watch tx (pack-tuple keys subspace))
        (transact! db #(.watch % (pack-tuple keys subspace))))
      (.thenRun callback)))
