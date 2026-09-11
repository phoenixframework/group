(ns group.jepsen.qualification-test
  (:require [clojure.test :refer :all]
            [group.jepsen.qualification :as qualification]))

(deftest corruption-must-be-injected-and-reach-its-check
  (doseq [[mode field] [["unexpected-death" :unexpected-owner-deaths]]]
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

(deftest internal-corruption-requires-specific-completion-and-invariant
  (doseq [[mode invariant] qualification/internal-invariants]
    (let [test {:corruption (name mode)}
          op {:f :corrupt :type :ok
              :value {:node "n1" :request {:mode mode}
                      :response {:injected mode}}}
          internal {:failed-invariants [invariant] :injected-corruptions [mode]}
          result {:valid? false :internal-invariant-errors {"n1" internal}}
          qualifies? #(qualification/qualified? test [%1] %2)]
      (is (qualifies? op result))
      (is (not (qualifies? (assoc op :type :fail) result)))
      (is (not (qualifies? (assoc-in op [:value :node] "n2") result)))
      (is (not (qualifies? op (assoc-in result
                                       [:internal-invariant-errors "n1" :failed-invariants]
                                       [:unrelated-invariant]))))
      (is (not (qualifies? (update-in op [:value] dissoc :response)
                          (assoc-in result
                                    [:internal-invariant-errors "n1" :injected-corruptions]
                                    [])))))))

(deftest arming-cursor-injection-with-no-remote-cursor-is-not-qualification
  (let [test {:corruption "cursor-marker"}
        history [{:f :corrupt :type :ok
                  :value {:node "n1" :request {:mode :cursor-marker}
                          :response {:status :ok}}}]
        result {:valid? false
                :internal-invariant-errors
                {"n1" {:healthy false
                       :errors ["invariant snapshot failed: no remote replica cursor available for corruption"]
                       :failed-invariants []
                       :injected-corruptions []
                       :snapshot-staging-count -1}}}]
    (is (not (qualification/qualified? test history result)))
    ;; Even a matching assertion elsewhere cannot replace injection completion.
    (is (not (qualification/qualified?
               test history
               (assoc-in result [:internal-invariant-errors "n1" :failed-invariants]
                         [:cursor-snapshot-marker]))))))

(deftest terminal-qualification-requires-retirement-and-missing-target
  (let [test {:corruption "terminal-unavailable" :terminal-nodes [:n1 :n2 :n3]}
        history [{:f :retire-node :type :info :value {:retired :n1}}]]
    (is (qualification/qualified? test history {:missing-nodes #{"n1"}}))
    (is (not (qualification/qualified? test [] {:missing-nodes #{"n1"}})))
    (is (not (qualification/qualified? test history {:missing-nodes #{"n2"}})))))
