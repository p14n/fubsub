(ns fdb-test (:require [p14n.fubsub.consumer :as consumer]
                       [p14n.fubsub.processor :as processor]
                       [p14n.fubsub.core :as core]
                       [p14n.fubsub.common :as common :refer [consumer-head-key-part
                                                              consumer-processing-key-part
                                                              topic-key-part
                                                              topic-head-key-part
                                                              processor-status-available
                                                              processor-status-processing]]
                       [clojure.test :refer [deftest is testing]]
                       [p14n.fubsub.data :as d]
                       [p14n.fubsub.producer :as producer]
                       [p14n.fubsub.util :as u]
                       [p14n.fubsub.logging :as log]))

(def topic1 "topic1")
(def consumer1 "consumer1")
(def node1 "node1")

(defn add-messages-to-db
  ([] (add-messages-to-db []))
  ([subspace]
   (d/with-transaction {}
     #(do
        (d/put-all {:tx % :logger (log/->StdoutLogger)}
                   (->> [[[consumer-head-key-part topic1 consumer1] ["msg02"]]
                         [[topic-key-part topic1 "msg01" "001"] ["msg01"]]
                         [[topic-key-part topic1 "msg02" "002"] ["msg02"]]
                         [[topic-key-part topic1 "msg03" "003"] ["msg03"]]
                         [[topic-key-part topic1 "msg04" "001"] ["msg04"]]
                         [[topic-key-part topic1 "msg05" "002"] ["msg05"]]
                         [[topic-key-part topic1 "msg06" "003"] ["msg06"]]
                         [[topic-key-part topic1 "msg07" "001"] ["msg07"]]
                         [[topic-key-part topic1 "msg08" "002"] ["msg08"]]
                         [[topic-key-part topic1 "msg09" "003"] ["msg09"]]]
                        (mapv (fn [[k v]] [(concat subspace k) v]))))))))

;

(defn wipe-db
  ([] (wipe-db []))
  ([subspace] (wipe-db subspace topic1))
  ([subspace topic]
   (d/with-transaction {}
     #(do
        (d/-clear-key-starts-with % (concat subspace [topic-head-key-part topic]) nil)
        (d/-clear-key-starts-with % (concat subspace [consumer-head-key-part topic]) nil)
        (d/-clear-key-starts-with % (concat subspace [topic-key-part topic]) nil)
        (d/-clear-key-starts-with % (concat subspace [consumer-processing-key-part topic]) nil)))))

"fubsub" "deterministic" "cp" "mytopic" "myconsumer"

(def notify-processors-simple
  (fn [{:keys [handlers] :as ctx} {:keys [topic consumer node msgs]}]
    (doseq [msg msgs]
      (doseq [handler (get handlers topic)]
        (when handler
          (let [[_ _ messageid key] (u/key-without-subspace ctx (first msg))]
            (processor/process-message ctx {:topic topic
                                            :consumer consumer
                                            :node node
                                            :key key
                                            :messageid messageid
                                            :handler handler
                                            :handler-name "hondler"})))))))

(deftest simple-test
  (testing "System reads all messages and marks the consumer head"
    (wipe-db)
    (add-messages-to-db)
    (let [results (atom [])
          handlers {topic1 [(fn [{:keys [logger]} msg]
                              (log/info logger :fdb-test/simple-test msg)
                              (swap! results conj msg))]}
          context {:threads 10
                   :current-timestamp-function (constantly "2024-08-08T14:48:26.715-00:00")
                   :notify-processors notify-processors-simple
                   :handlers handlers
                   :logger (log/->StdoutLogger)
                   :get-range-after d/get-range-after
                   :get-value d/get-value
                   :get-range-before d/get-range-before
                   :put-all d/put-all
                   :compare-and-clear d/compare-and-clear
                   :tx-wrapper d/with-transaction
                   :id-formatter d/versionstamp->id-string}]
      (consumer/topic-check context {:topic topic1
                                     :consumer consumer1
                                     :node "node1"})

      (is (= ["msg03" "msg04" "msg05" "msg06" "msg07" "msg08" "msg09"]
             (->> @results (map :data))))
      (is (= ["msg09"]
             (d/with-transaction {}
               #(d/get-value {:tx % :logger (log/->StdoutLogger)}
                             [consumer-head-key-part topic1 consumer1])))))))

(deftest simple-test-with-subspace
  (testing "System reads all messages and marks the consumer head"
    (wipe-db ["the" "subspace"])
    (add-messages-to-db ["the" "subspace"])
    (let [results (atom [])
          handlers {topic1 [(fn [{:keys [logger]} msg]
                              (log/info logger :fdb-test/simple-test-with-subspace msg)
                              (swap! results conj msg))]}
          context {:threads 10
                   :current-timestamp-function (constantly "2024-08-08T14:48:26.715-00:00")
                   :notify-processors notify-processors-simple
                   :handlers handlers
                   :logger (log/->StdoutLogger)
                   :get-range-after d/get-range-after
                   :get-value d/get-value
                   :get-range-before d/get-range-before
                   :put-all d/put-all
                   :compare-and-clear d/compare-and-clear
                   :tx-wrapper d/with-transaction
                   :id-formatter d/versionstamp->id-string
                   :subspace ["the" "subspace"]}]
      (consumer/topic-check context {:topic topic1
                                     :consumer consumer1
                                     :node "node1"})

      (is (= ["msg03" "msg04" "msg05" "msg06" "msg07" "msg08" "msg09"]
             (->> @results (map :data))))
      (is (= ["msg09"]
             (d/with-transaction {}
               #(d/get-value {:tx %
                              :subspace ["the" "subspace"]
                              :logger (log/->StdoutLogger)} ["the" "subspace" consumer-head-key-part topic1 consumer1])))))))

(deftest send-receive-message
  (testing "Produce a message and read it back"
    (wipe-db)
    (producer/put-message topic1 "hello" "Dean")
    (let [results (atom [])
          handlers {topic1 [(fn [{:keys [logger]} msg]
                              (log/info logger :fdb-test/send-receive-message msg)
                              (swap! results conj msg))]}
          context {:threads 10
                   :current-timestamp-function (constantly "2024-08-08T14:48:26.715-00:00")
                   :notify-processors notify-processors-simple
                   :handlers handlers
                   :logger (log/->StdoutLogger)
                   :get-range-after d/get-range-after
                   :get-value d/get-value
                   :get-range-before d/get-range-before
                   :put-all d/put-all
                   :compare-and-clear d/compare-and-clear
                   :tx-wrapper d/with-transaction
                   :id-formatter d/versionstamp->id-string}]
      (consumer/topic-check context {:topic topic1
                                     :consumer consumer1
                                     :node "node1"})

      (is (= "hello" (-> @results (first) :data))))))

(defn add-processing-to-db
  ([] (add-processing-to-db []))
  ([subspace]
   (d/with-transaction {}
     #(do
        (d/put-all {:tx % :logger (log/->StdoutLogger)}
                   (->> [[[consumer-processing-key-part topic1 consumer1 "msg01" "001"] [processor-status-available node1 "2024-08-08T14:48:24.000-00:00"]]
                         [[consumer-processing-key-part topic1 consumer1 "msg02" "002"] [processor-status-processing node1 "2024-08-08T14:48:24.000-00:00"]]
                         [[consumer-processing-key-part topic1 consumer1 "msg03" "001"] [processor-status-available node1 "2024-08-08T14:48:25.000-00:00"]]
                         [[consumer-processing-key-part topic1 consumer1 "msg04" "002"] [processor-status-processing node1 "2024-08-08T14:48:25.000-00:00"]]]
                        (mapv (fn [[k v]] [(concat subspace k) v]))))))))

(deftest test-resubmit-abandoned
  (testing "Old available messages are resubmitted"
    (wipe-db)
    (add-processing-to-db)
    (let [context {:current-timestamp-function (constantly "2024-08-08T14:48:24.500-00:00")
                   :get-range-after d/get-range-after
                   :tx-wrapper d/with-transaction
                   :logger (log/->StdoutLogger)}
          results (atom [])]
      (core/resubmit-abandoned context {:topic topic1
                                        :consumer consumer1
                                        :node node1}
                               (fn [_ _ to-resubmit]
                                 (reset! results to-resubmit)))
      (is (= [["msg01" "001"]]
             @results))))
  (testing "Old available and processing messages are resubmitted"
    (wipe-db)
    (add-processing-to-db)
    (let [context {:current-timestamp-function (constantly "2024-08-08T14:48:24.500-00:00")
                   :get-range-after d/get-range-after
                   :resubmit-processing-ms 2000
                   :put-all d/put-all
                   :tx-wrapper d/with-transaction
                   :logger (log/->StdoutLogger)}
          results (atom [])]
      (core/resubmit-abandoned context {:topic topic1
                                        :consumer consumer1
                                        :node node1}
                               (fn [_ _ to-resubmit]
                                 (reset! results to-resubmit)))
      (is (= [["msg01" "001"] ["msg02" "002"]]
             @results)))))