(ns processor-test
  (:require [clojure.test :refer [deftest is testing]]
            [test-util :as tu]
            [p14n.fubsub.processor :as processor]
            [p14n.fubsub.common :refer [consumer-processing-key-part
                                        processor-status-available
                                        processor-status-processing]]))

(def topic1 "topic1")
(def consumer1 "consumer1")
(def node1 "node1")

(defn reset-db []
  (reset! tu/db (into (sorted-map)
                      {[consumer-processing-key-part topic1 consumer1 "msg01" "001"] [processor-status-available node1]
                       [consumer-processing-key-part topic1 consumer1 "msg02" "001"] [processor-status-available node1]})))

(deftest testing-processor-in-flight-checks
  (reset-db)
  (testing "Processor finds an existing key in flight"
    (is (= ["msg01"]
           (processor/check-for-key-in-flight  {:topic topic1
                                                :consumer consumer1
                                                :messageid "msg02"
                                                :key "001"
                                                :get-range-before tu/get-range-before}))))
  (testing "Processor finds no existing key in flight"
    (reset! tu/db (into (sorted-map)
                        {[consumer-processing-key-part topic1 consumer1 "msg02" "001"] [processor-status-available node1]}))
    (is (= []
           (processor/check-for-key-in-flight  {:topic topic1
                                                :consumer consumer1
                                                :messageid "msg02"
                                                :key "001"
                                                :get-range-before tu/get-range-before})))))

(deftest testing-processor-marking
  (testing "Processor marks a message as processing"
    (reset-db)
    (processor/mark-as-processing {:topic topic1
                                   :consumer consumer1
                                   :messageid "msg01"
                                   :key "001"
                                   :node node1
                                   :update tu/put-all})
    (is (= {[consumer-processing-key-part topic1 consumer1 "msg01" "001"] [processor-status-processing node1]
            [consumer-processing-key-part topic1 consumer1 "msg02" "001"] [processor-status-available node1]}
           @tu/db)))

  (testing "Processor removes a message as processing"
    (reset-db)
    (processor/remove-processing-mark {:topic topic1
                                       :consumer consumer1
                                       :messageid "msg01"
                                       :key "001"
                                       :delete tu/delete-all})
    (is (= {[consumer-processing-key-part topic1 consumer1 "msg02" "001"] [processor-status-available node1]}
           @tu/db))))