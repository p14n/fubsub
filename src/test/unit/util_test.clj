(ns util-test
  (:require [clojure.test :refer [deftest is testing]]
            [p14n.fubsub.util :as sut]))

(deftest testing-date-string-comparison
  (testing "first-is-earliest - true"
    (is (sut/first-timestamp-is-earliest "2024-08-08T14:48:23.999-00:00"
                                         "2024-08-08T14:48:24.000-00:00")))
  (testing "first-is-earliest - false"
    (is (not (sut/first-timestamp-is-earliest "2024-08-08T14:48:24.000-00:00"
                                              "2024-08-08T14:48:23.999-00:00"))))
  (testing "first-is-earliest - false (equal)"
    (is (not (sut/first-timestamp-is-earliest "2024-08-08T14:48:23.999-00:00"
                                              "2024-08-08T14:48:23.999-00:00")))))
