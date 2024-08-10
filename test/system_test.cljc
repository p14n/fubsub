(ns system-test
  (:require [p14n.fubsub.consumer :as consumer]
            [p14n.fubsub.processor :as processor]
            [p14n.fubsub.common :as common :refer [consumer-head-key-part
                                                   topic-key-part]]
            [clojure.test :refer [deftest is testing]]
            [test-util :as tu]))

(def topic1 "topic1")
(def consumer1 "consumer1")

(defn reset-db []
  (reset! tu/db (into (sorted-map) {[consumer-head-key-part topic1 consumer1] ["msg02"]
                                    [topic-key-part topic1 "msg01" "001"] ["msg01"]
                                    [topic-key-part topic1 "msg02" "002"] ["msg02"]
                                    [topic-key-part topic1 "msg03" "003"] ["msg03"]
                                    [topic-key-part topic1 "msg04" "001"] ["msg04"]
                                    [topic-key-part topic1 "msg05" "002"] ["msg05"]
                                    [topic-key-part topic1 "msg06" "003"] ["msg06"]
                                    [topic-key-part topic1 "msg07" "001"] ["msg07"]
                                    [topic-key-part topic1 "msg08" "002"] ["msg08"]
                                    [topic-key-part topic1 "msg09" "003"] ["msg09"]})))

(def notify-processors-simple
  (fn [{:keys [handlers] :as ctx} {:keys [topic consumer node msgs]}]
    (doseq [msg msgs]
      (doseq [handler (get handlers topic)]
        (when handler
          (processor/process-message ctx {:topic topic
                                          :consumer consumer
                                          :node node
                                          :msg msg
                                          :handler handler}))))))

(deftest simple-test
  (testing "System reads all messages and marks the consumer head"
    (reset-db)
    (let [results (atom [])
          handlers {topic1 [(fn [_ msg]
                              (println "Received" topic1 msg)
                              (swap! results conj msg))]}
          context {:threads 10
                   :current-timestamp-function (constantly "2024-08-08T14:48:26.715-00:00")
                   :notify-processors notify-processors-simple
                   :handlers handlers
                   :error-log println
                   :info-log println
                   :get-range-after tu/get-range-after
                   :get-value tu/get-value
                   :get-range-before tu/get-range-before
                   :put-all tu/put-all
                   :compare-and-clear tu/compare-and-clear
                   :tx-wrapper (fn [_ f] (f nil))
                   :id-formatter str}]
      (consumer/topic-check context {:topic topic1
                                     :consumer consumer1
                                     :node "node1"})
      (is (= ["msg03" "msg04" "msg05" "msg06" "msg07" "msg08" "msg09"]
             (->> @results (map :data) (vec))))
      (is (= [[consumer-head-key-part topic1 consumer1] ["msg09"]]
             (first @tu/db))))))

