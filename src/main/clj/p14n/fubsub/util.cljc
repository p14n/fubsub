(ns p14n.fubsub.util
  (:import [java.time.format DateTimeFormatter]
           [java.time LocalDateTime ZoneOffset]))

(defn assoc-if [m k v]
  (if v (assoc m k v) m))

(def df (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss.SSSX"))

(defn current-timestamp-string
  ([] (current-timestamp-string
       (-> (LocalDateTime/now)
           (.atOffset ZoneOffset/UTC))))
  ([t]
   (-> t
       (.format df))))

(defmacro quickmap
  "Create a map with keys matching parameter names and values matching parameter values"
  [& args]
  (let [p (cons 'list (map (juxt (comp keyword name) identity) args))]
    `(into {} ~p)))

(defn ctx-with-tx [ctx tx]
  (assoc ctx :tx tx))

(defn key-without-subspace [{:keys [subspace]} key]
  (drop (count subspace) key))

(defn now-minus-ms [ms]
  (-> (LocalDateTime/now)
      (.atOffset ZoneOffset/UTC)
      (.minusNanos (* 1000000 ms))))

(defn first-timestamp-is-earliest [t1 t2] (println ">???" t1 t2) (> 0 (compare t1 t2)))

