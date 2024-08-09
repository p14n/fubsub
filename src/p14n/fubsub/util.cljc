(ns p14n.fubsub.util
  (:import [java.time.format DateTimeFormatter]
           [java.time LocalDateTime ZoneOffset]))

(defn assoc-if [m k v]
  (if v (assoc m k v) m))


(def df (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss.SSSX"))

(defn current-timestamp-string []
  (-> (LocalDateTime/now)
      (.atOffset ZoneOffset/UTC)
      (.format df)))

(defmacro quickmap
  "Create a map with keys matching parameter names and values matching parameter values"
  [& args]
  (let [p (cons 'list (map (juxt (comp keyword name) identity) args))]
    `(into {} ~p)))
