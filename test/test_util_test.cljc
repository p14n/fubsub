(ns test-util-test
  (:require [clojure.test :refer [deftest is testing]]
            [test-util :refer [key-range >key-range]]))

(deftest testing-key-range
  (testing "key-range selects all subkeys of the key"
    (is (= (key-range [1 2] {[1 2 3] 1,
                             [1 2] 2,
                             [3 4] 3})
           [[[1 2 3] 1]
            [[1 2] 2]])))
  (testing ">key-range selects all keys greater than the key"
    (is (= (>key-range [1 2 3] {[1 2 2] 0,
                                [1 2 3] 1,
                                [1 2 3 1] 1,
                                [1 2 4] 2,
                                [1 2 5] 3
                                [1] 4
                                [1 3 4] 5}
                       10)
           [[[1 2 4] 2]
            [[1 2 5] 3]]))))
