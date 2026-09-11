(ns group.jepsen.qualification
  (:require [jepsen.checker :as checker]))

(def internal-invariants
  {:internal-index :registry-dual-indexes
   :cursor-marker :cursor-snapshot-marker
   :registry-projection :registry-projection})

(defn internal-qualified? [mode history result]
  ;; Match completion and the precise assertion on the same injected node.
  ;; Arming the cursor marker file is not injection: only a successful snapshot
  ;; insertion can attest that a remote cursor existed and was corrupted.
  (some (fn [op]
          (let [internal (get-in result [:internal-invariant-errors
                                        (get-in op [:value :node])])
                completed? (if (= :cursor-marker mode)
                             (some #{mode} (:injected-corruptions internal))
                             (= mode (get-in op [:value :response :injected])))]
            (and (= :corrupt (:f op))
                 (= :ok (:type op))
                 (= mode (get-in op [:value :request :mode]))
                 completed?
                 (some #{(internal-invariants mode)} (:failed-invariants internal)))))
        history))

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
        :internal-index (internal-qualified? mode history result)
        :cursor-marker (internal-qualified? mode history result)
        :registry-projection (internal-qualified? mode history result)
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
