(ns fdb-deterministic-test
  (:require [clojure.test.check.generators :as gen]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.properties :as prop]
            [p14n.fubsub.logging :as log]
            [p14n.fubsub.core :as c]
            [p14n.fubsub.producer :as p]
            [p14n.fubsub.processor :as proc]
            [pseudo-concurrency :as pccy]
            [p14n.fubsub.common :as co]
            [p14n.fubsub.concurrency :as ccy]
            [p14n.fubsub.data :as d]
            [p14n.fubsub.util :as u]
            [fdb-test :as fdbt])
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

(def minimal-config {:handlers {"mytopic" [minimal-handler]}
                     :consumer-name "myconsumer"
                     :logger (->TestLogger)
                     :subspace ["fubsub" "deterministic"]
                     :fetch-size 10
                     :resubmit-available-ms 10})

(defn check-for-completion [ctx]
  (d/with-transaction ctx
    #(let [ctx-tx (assoc (u/ctx-with-tx ctx %) :get-range-after d/get-range-after)
           consumer-head (d/get-value ctx-tx [co/consumer-head-key-part "mytopic" "myconsumer"])
           topic-head (d/get-value ctx-tx [co/topic-head-key-part "mytopic"])
           processing (proc/get-all-processing ctx-tx {:topic "mytopic"
                                                       :consumer "myconsumer"})]
       (and (= (-> consumer-head (vec) (first) (str)) (-> topic-head (vec) (first) (str)))
            (not (seq processing))))))

(defn test-consumer
  ([seed message-count executions-per-loop max-execution-loops]
   (reset! loglines [])
   (pccy/reset-threads)
   (fdbt/wipe-db ["fubsub" "deterministic"] "mytopic")
   (with-redefs-fn {#'p14n.fubsub.concurrency/run-async pccy/run-async
                    #'p14n.fubsub.concurrency/run-async-while pccy/run-async-while
                    #'p14n.fubsub.concurrency/acquire-semaphore pccy/acquire-semaphore}
     #(let [ctx (assoc minimal-config :db (d/open-db))
            _ (c/start-consumer ctx)
            r (Random. seed)]
        (doseq [msg-idx (range message-count)]
          (p/put-message "mytopic" (str "msg" msg-idx) (str "subject-key" (mod msg-idx 10)) {:subspace ["fubsub" "deterministic"]}))
        (loop [outer-idx max-execution-loops]
          (doseq [_ (range executions-per-loop)]
            (let [[pred f] (pccy/shuffle-and-pop-first-thread-function r)]
              (when (pred)
                (f))))
          (if (or (>= 0 outer-idx) (check-for-completion ctx))
            (println "Completed with" (* executions-per-loop outer-idx) " thread executions")
            (recur (dec outer-idx))))
        (let [processed (->> @loglines
                             (map :body)
                             (map :event)
                             (remove nil?)
                             (group-by identity)
                             :handler/processed
                             (count))
              _ (println "Processed" processed "messages")]
          {:pass (= message-count processed)
           :processed processed})))))

(defspec run-consumer-in-pseudo-concurrency
  10
  (prop/for-all [thread-seed gen/large-integer]
                (-> (test-consumer thread-seed 51 20 200)
                    :pass)))
