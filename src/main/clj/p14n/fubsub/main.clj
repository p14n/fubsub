(ns p14n.fubsub.main
  (:require [p14n.fubsub.core :as c]
            [p14n.fubsub.concurrency :as ccy]
            [p14n.fubsub.logging :as log]))

(defn minimal-consumer
  [{:keys [logger]} msg]
  (log/info logger :main/minimal-consumer msg))

(def minimal-config {:handlers {"mytopic" [minimal-consumer]
                                "yourtopic" [minimal-consumer]}
                     :consumer-name "myconsumer"
                     :subspace ["fubsub" "test"]})

(defn wait []
  (while (not (.isShutdown ccy/executor))
    (Thread/sleep 1000)))

(defn -main
  ([]
   (c/start-consumer minimal-config)
   (wait))
  ([config-file]
   (c/start-consumer (some-> config-file
                             (slurp)
                             (read-string)))
   (wait)))