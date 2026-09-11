(ns group.jepsen.qualification-test
  (:require [clojure.test :refer :all]
            [group.jepsen.qualification :as qualification]))

(deftest corruption-must-be-injected-and-reach-its-check
  (doseq [[mode field] [["unexpected-death" :unexpected-owner-deaths]
                       ["internal-index" :internal-invariant-errors]
                       ["cursor-marker" :internal-invariant-errors]
                       ["registry-projection" :internal-invariant-errors]]]
    (let [test {:corruption mode}
          history [{:f :corrupt :type :ok
                    :value {:request {:mode (keyword mode)}}}]
          result {:valid? false field {:n1 :evidence}}]
      (is (qualification/qualified? test history result))
      (is (not (qualification/qualified? test [] result)))
      (is (not (qualification/qualified? test history {:valid? false})))
      (is (not (qualification/qualified? test
                                       [(assoc (first history) :type :fail)]
                                       result))))))

(deftest terminal-qualification-requires-retirement-and-missing-target
  (let [test {:corruption "terminal-unavailable" :terminal-nodes [:n1 :n2 :n3]}
        history [{:f :retire-node :type :info :value {:retired :n1}}]]
    (is (qualification/qualified? test history {:missing-nodes #{"n1"}}))
    (is (not (qualification/qualified? test [] {:missing-nodes #{"n1"}})))
    (is (not (qualification/qualified? test history {:missing-nodes #{"n2"}})))))
