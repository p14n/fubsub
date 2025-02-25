(ns fdb-deterministic-test
  (:require [clojure.core :refer [with-redefs-fn]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [fdb-test :as fdbt]
            [p14n.fubsub.common :as co]
            [p14n.fubsub.concurrency :as ccy]
            [p14n.fubsub.core :as c]
            [p14n.fubsub.data :as d]
            [p14n.fubsub.logging :as log]
            [p14n.fubsub.processor :as proc]
            [p14n.fubsub.producer :as p]
            [p14n.fubsub.util :as u]
            [pseudo-concurrency :as pccy])
  (:import [java.util Random]))

(def loglines (atom []))

(defrecord TestLogger []
  log/Logger
  (log [_ msg]
    (println msg)
    (swap! loglines conj msg)))

(defn minimal-handler
  [{:keys [logger]} _]
  (log/info logger :main/minimal-consumer {:event :handler/processed}))

(defn minimal-handler-2
  [{:keys [logger]} _]
  (log/info logger :main/minimal-consumer {:event :handler/processed}))

(def minimal-config {:handlers {"mytopic" [minimal-handler]}
                     :consumer-name "myconsumer"
                     :logger (->TestLogger)
                     :subspace ["fubsub" "deterministic"]
                     :fetch-size 10
                     :resubmit-available-ms 10})

(def two-handlers-cfg (assoc minimal-config :handlers {"mytopic" [minimal-handler
                                                                  minimal-handler-2]}))

(defn check-for-completion [ctx]
  (d/with-transaction ctx
    #(let [ctx-tx (assoc (u/ctx-with-tx ctx %) :get-range-after d/get-range-after)
           consumer-head (d/get-value ctx-tx [co/consumer-head-key-part "mytopic" "myconsumer"])
           topic-head (d/get-value ctx-tx [co/topic-head-key-part "mytopic"])
           processing (proc/get-all-processing ctx-tx {:topic "mytopic"
                                                       :consumer "myconsumer"})]
       (println "Check for completion: consumer-head" consumer-head ", topic-head" topic-head "processing" (count processing))
       (and (= (-> consumer-head (vec) (first) (str)) (-> topic-head (vec) (first) (str)))
            (not (seq processing))))))

(defn test-consumer
  ([seed cfg message-count expected-processed executions-per-loop max-execution-loops]
   (reset! loglines [])
   (pccy/reset-threads)
   (fdbt/wipe-db ["fubsub" "deterministic"] "mytopic")
   (with-redefs-fn {#'p14n.fubsub.concurrency/run-async pccy/run-async
                    #'p14n.fubsub.concurrency/run-async-while pccy/run-async-while
                    #'p14n.fubsub.concurrency/acquire-semaphore pccy/acquire-semaphore}
     #(let [ctx (assoc cfg :db (d/open-db))
            r (Random. seed)]
        (c/start-consumer ctx)
        (doseq [msg-idx (range message-count)]
          (p/put-message "mytopic" (str "msg" msg-idx) (str "subject-key" (mod msg-idx 10)) {:subspace ["fubsub" "deterministic"]}))
        (loop [outer-idx max-execution-loops]
          (doseq [_ (range executions-per-loop)]
            (let [[pred f] (pccy/shuffle-and-pop-first-thread-function r)]
              (when (pred)
                (f))))
          (if (or (>= 0 outer-idx) (check-for-completion ctx))
            (println "Completed with" (* executions-per-loop (- max-execution-loops outer-idx)) " thread executions")
            (recur (dec outer-idx))))
        (let [processed (->> @loglines
                             (map :body)
                             (map :event)
                             (remove nil?)
                             (group-by identity)
                             :handler/processed
                             (count))
              _ (println "Processed" processed "messages")]
          {:pass (= expected-processed processed)
           :processed processed})))))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(defspec run-consumer-in-pseudo-concurrency
  10
  (prop/for-all [thread-seed gen/large-integer]
                (-> (test-consumer thread-seed minimal-config 11 11 10 100)
                    :pass)))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(defspec run-two-handlers-in-pseudo-concurrency
  10
  (prop/for-all [thread-seed gen/large-integer]
                (-> (test-consumer thread-seed two-handlers-cfg 11 22 10 100)
                    :pass)))
