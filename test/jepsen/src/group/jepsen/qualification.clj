(ns group.jepsen.qualification
  (:require [jepsen.checker :as checker]))

(defn qualified? [test history result]
  (let [mode (keyword (:corruption test))
        injected? (some #(and (= :corrupt (:f %))
                             (= :ok (:type %))
                             (= mode (get-in % [:value :request :mode])))
                        history)]
    (boolean
      (case mode
        :none (true? (:valid? result))
        :unexpected-death (and injected? (seq (:unexpected-owner-deaths result)))
        :internal-index (and injected? (seq (:internal-invariant-errors result)))
        :cursor-marker (and injected? (seq (:internal-invariant-errors result)))
        :registry-projection (and injected? (seq (:internal-invariant-errors result)))
        :terminal-unavailable
        (let [target (first (:terminal-nodes test))]
          (and (some #(and (= :retire-node (:f %))
                           (= :info (:type %))
                           (= target (get-in % [:value :retired])))
                     history)
               (contains? (:missing-nodes result) (name target))))
        false))))

(defn checker [delegate]
  (reify checker/Checker
    (check [_ test history opts]
      (let [result (checker/check delegate test history opts)]
        ;; A dedicated per-run artifact, never human log text. Exceptions and
        ;; indeterminate results cannot certify a completed checker decision.
        (when-let [path (System/getenv "GROUP_JEPSEN_QUALIFICATION_RESULT")]
          (when (boolean? (:valid? result))
            (spit path (str "group-qualification-v1\t" (:corruption test) "\t"
                            (:valid? result) "\t"
                            (qualified? test history result) "\n"))))
        result))))
