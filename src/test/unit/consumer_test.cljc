(ns consumer-test
  (:require [clojure.test :refer [deftest is testing]]
            [test-util :as tu]
            [p14n.fubsub.common :refer [consumer-head-key-part
                                        consumer-processing-key-part
                                        topic-key-part
                                        processor-status-available]]
            [p14n.fubsub.consumer :as consumer]))

;; Consumer loop
;; - Waits for new messages
;; - Then waits for new threads (maybe reverse these - dont grab what you cant handle)
;; - Grabs as many messages as it can handle immediately
;; - Inserts to processing and moves the head up
;; - Fetches unprocessed and assigns to processors (include errored some time ago)
;; - where topic, consumer, status, nodeid
;; - Loops

(def topic1 "topic1")
(def consumer1 "consumer1")
(def node1 "node1")

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

(deftest testing-consumer
  (reset-db)
  (testing "Consumer selects new messages"
    (let [msgs (consumer/select-new-messages {:get-range-after tu/get-range-after
                                              :get-value tu/get-value
                                              :threads 10}
                                             {:topic topic1
                                              :consumer consumer1
                                              :node node1})]
      (is (= [[["tp" "topic1" "msg03" "003"] ["msg03"]]
              [["tp" "topic1" "msg04" "001"] ["msg04"]]
              [["tp" "topic1" "msg05" "002"] ["msg05"]]
              [["tp" "topic1" "msg06" "003"] ["msg06"]]
              [["tp" "topic1" "msg07" "001"] ["msg07"]]
              [["tp" "topic1" "msg08" "002"] ["msg08"]]
              [["tp" "topic1" "msg09" "003"] ["msg09"]]] msgs))
      (is (= [[[consumer-head-key-part topic1 consumer1] ["msg09"]]
              [[consumer-processing-key-part topic1 consumer1 "msg03" "003" "hondler"] [processor-status-available node1 "2024-08-08T14:48:26.715-00:00"]]
              [[consumer-processing-key-part topic1 consumer1 "msg04" "001" "hondler"] [processor-status-available node1 "2024-08-08T14:48:26.715-00:00"]]
              [[consumer-processing-key-part topic1 consumer1 "msg05" "002" "hondler"] [processor-status-available node1 "2024-08-08T14:48:26.715-00:00"]]
              [[consumer-processing-key-part topic1 consumer1 "msg06" "003" "hondler"] [processor-status-available node1 "2024-08-08T14:48:26.715-00:00"]]
              [[consumer-processing-key-part topic1 consumer1 "msg07" "001" "hondler"] [processor-status-available node1 "2024-08-08T14:48:26.715-00:00"]]
              [[consumer-processing-key-part topic1 consumer1 "msg08" "002" "hondler"] [processor-status-available node1 "2024-08-08T14:48:26.715-00:00"]]
              [[consumer-processing-key-part topic1 consumer1 "msg09" "003" "hondler"] [processor-status-available node1 "2024-08-08T14:48:26.715-00:00"]]]
             (consumer/select-new-messages-tx {:current-timestamp-function (constantly "2024-08-08T14:48:26.715-00:00")
                                               :handlers {topic1 [(with-meta (fn [_ _]) {:handler-name "hondler"})]}}
                                              {:topic topic1
                                               :consumer consumer1
                                               :node node1
                                               :msgs msgs}))))))