(ns p14n.fubsub.main
  (:require [p14n.fubsub.core :as c]
            [p14n.fubsub.concurrency :as ccy]))

(defn minimal-consumer
  [{:keys [topic]} msg]
  (println topic "received" msg))

(def minimal-config {:handlers {"mytopic" [minimal-consumer]
                                "yourtopic" [minimal-consumer]}
                     :consumer-name "myconsumer"})

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