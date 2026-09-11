(ns group.jepsen.model-test
  (:require [clojure.test :refer :all]
            [clojure.edn :as edn]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [group.jepsen.model :as model]))

(def test-map
  {:nodes ["n1" "n2" "n3"]
   :terminal-nodes ["n1" "n2" "n3"]
   :key-count 2
   :clusters ["red"]
   :transport "distribution"
   :terminal-snapshots-per-node 1
   :required-transport-events #{}})

(defn peers-for [node nodes]
  (->> nodes
       (remove #(= node %))
       (map #(str "group@" %))
       sort
       vec))

(defn public-view [root red]
  {"root" root, "red" red})

(defn healthy-internal []
  {:healthy true
   :errors []
   :snapshot-staging-count 0
   :oplog-entries 2})

(defn snapshot-op
  ([index node owners registry pg]
   (snapshot-op index node (:terminal-nodes test-map) owners registry pg))
  ([index node nodes owners registry pg]
   {:index index
    :process index
    :type :ok
    :f :snapshot
    :value {:node node
            :peers (peers-for node nodes)
            :owners owners
            :unexpected-deaths []
            :transport-events {}
            :transport-profile :distribution
            :internal (healthy-internal)
            :registry registry
            :pg pg}}))

(defn owner [token registrations memberships]
  {:token token, :registrations registrations, :memberships memberships})

(defn registration [cluster key revision]
  {:cluster cluster, :key key, :revision revision})

(defn membership [cluster key revision]
  {:cluster cluster, :key key, :revision revision})

(defn empty-registry []
  (public-view {0 nil, 1 nil} {0 nil, 1 nil}))

(defn empty-pg []
  (public-view {0 [], 1 []} {0 [], 1 []}))

(defn with-unexpected-death [op token]
  (assoc-in op [:value :unexpected-deaths] [{:token token, :reason ":boom"}]))

(def conflict-journal
  [{:kind :register :sequence 1 :token "a" :cluster nil :key 0 :revision 1}
   {:kind :result :sequence 2 :attempt 1 :status :ok}
   {:kind :register :sequence 3 :token "b" :cluster nil :key 0 :revision 2}
   {:kind :death :sequence 4 :token "a" :key "jepsen/registry/0"
    :winner {:token "b" :revision 2}}
   {:kind :result :sequence 5 :attempt 3 :status :ok}
   {:kind :unregister :sequence 6 :token "b" :cluster nil :key 0}])

(defn with-conflict-evidence [op]
  (assoc-in op [:value :conflict-evidence] conflict-journal))

(deftest validates-historical-conflicts-independently
  (let [check #(model/conflict-analysis {"n1" {:conflict-evidence %}})
        valid #(is (empty? (:invalid (check %))))
        invalid #(is (= 1 (count (:invalid (check %)))))]
    (valid conflict-journal)
    (valid (vec (remove #(= 2 (:sequence %)) conflict-journal)))
    (invalid (assoc-in conflict-journal [3 :key] "nonexistent"))
    (invalid (assoc-in conflict-journal [3 :winner :revision] -100))
    (invalid (assoc-in conflict-journal [3 :winner :token] "invented"))
    (invalid (assoc-in conflict-journal [3 :winner :token] "a"))
    (invalid (assoc-in conflict-journal [2 :cluster] "red"))
    (invalid (assoc-in conflict-journal [4 :status] :fail))
    (invalid (assoc-in conflict-journal [0 :revision] 99))
    (invalid (vec (concat (subvec conflict-journal 0 3)
                          [{:kind :unregister :sequence 4 :token "a" :cluster nil :key 0}]
                          (subvec conflict-journal 3))))
    (invalid (vec (concat (subvec conflict-journal 0 3)
                          [{:kind :drop-cluster :sequence 4 :token "a" :cluster nil}]
                          (subvec conflict-journal 3))))
    (invalid (conj conflict-journal
                   {:kind :register :sequence 7 :token "b" :cluster nil :key 0 :revision 2}
                   {:kind :death :sequence 8 :token "b" :key "jepsen/registry/0"
                    :winner {:token "a" :revision 1}}))
    (invalid (vec (remove #(= :register (:kind %)) conflict-journal)))
    (invalid (vec (concat (subvec conflict-journal 0 2)
                          [(last conflict-journal)]
                          (subvec conflict-journal 3 4)
                          [(assoc (get conflict-journal 2) :sequence 5)])))
    ;; Unique incarnation tokens break equal-revision ties without pid order.
    (valid (assoc-in conflict-journal [0 :revision] 2))))

(deftest conflict-statistics-alone-do-not-discharge-deaths
  (let [history [(assoc-in (snapshot-op 1 "n1" [] (empty-registry) (empty-pg))
                           [:value :transport-events] {:registry-conflict-death 99})
                 (snapshot-op 2 "n2" [] (empty-registry) (empty-pg))
                 (snapshot-op 3 "n3" [] (empty-registry) (empty-pg))]
        result (model/analyze (assoc test-map :required-transport-events
                                    #{:registry-conflict-death}) history)]
    (is (false? (:valid? result)))
    (is (= #{:registry-conflict-death} (:missing-transport-events result)))))

(deftest checks-real-owner-and-driver-evidence
  (let [{:keys [exit out err]}
        (shell/sh "mix" "run" "--no-start" "test/jepsen/conflict_probe.exs"
                  :dir "../.."
                  :env (assoc (into {} (System/getenv)) "ERL_FLAGS" "+S 2:2"))
        _ (is (zero? exit) (str out err))
        line (first (filter #(str/starts-with? % "CONFLICT-PROBE ")
                            (str/split-lines out)))
        scenarios (when line (edn/read-string (subs line 15)))]
    (is (some? scenarios) (str out err))
    (doseq [{:keys [label valid snapshots]} scenarios]
      (let [history (mapv (fn [index node]
                            (assoc-in (snapshot-op index node [] (empty-registry) (empty-pg))
                                      [:value :conflict-evidence]
                                      (get-in snapshots [node :conflict-evidence])))
                          (range 3) ["n1" "n2" "n3"])
            result (model/analyze test-map history)]
        (is (= valid (:valid? result)) (str label ": " result))))))

(deftest accepts-an-exact-converged-multi-cluster-view
  (let [owners [(owner "a" [(registration nil 0 1) (registration "red" 1 2)] [])
                (owner "b" [] [(membership nil 1 2) (membership "red" 0 3)])]
        registry (public-view {0 "a", 1 nil} {0 nil, 1 "a"})
        pg (public-view {0 [], 1 ["b"]} {0 ["b"], 1 []})
        history [(snapshot-op 1 "n1" owners registry pg)
                 (snapshot-op 2 "n2" [] registry pg)
                 (snapshot-op 3 "n3" [] registry pg)]]
    (is (:valid? (model/analyze test-map history)))))

(deftest does-not-require-an-owner-without-group-intent
  (let [idle-owner (owner "idle" [] [])
        history [(snapshot-op 1 "n1" [idle-owner] (empty-registry) (empty-pg))
                 (snapshot-op 2 "n2" [] (empty-registry) (empty-pg))
                 (snapshot-op 3 "n3" [] (empty-registry) (empty-pg))]]
    (is (:valid? (model/analyze test-map history)))))

(deftest rejects-an-incomplete-terminal-observation
  (let [history [(snapshot-op 1 "n1" [] (empty-registry) (empty-pg))
                 (snapshot-op 2 "n2" [] (empty-registry) (empty-pg))]
        result (model/analyze test-map history)]
    (is (false? (:valid? result)))
    (is (= #{"n3"} (:missing-nodes result)))))

(deftest accepts-a-permanently-retired-node-and-requires-its-absence
  (let [survivors ["n2" "n3"]
        permanent-test (assoc test-map :terminal-nodes survivors)
        history [(snapshot-op 1 "n2" survivors [] (empty-registry) (empty-pg))
                 (snapshot-op 2 "n3" survivors [] (empty-registry) (empty-pg))]]
    (is (:valid? (model/analyze permanent-test history)))))

(deftest rejects-zombies-missing-live-owners-and-divergence
  (let [live (owner "live" [(registration nil 0 1)] [])
        stale-registry (assoc-in (empty-registry) ["root" 0] "dead")
        result (model/analyze
                 test-map
                 [(snapshot-op 1 "n1" [live] stale-registry (empty-pg))
                  (snapshot-op 2 "n2" [] (empty-registry) (empty-pg))
                  (snapshot-op 3 "n3" [] (empty-registry) (empty-pg))])]
    (is (false? (:valid? result)))
    (is (= #{"dead"} (:orphaned-owner-tokens result)))
    (is (= #{"live"} (:missing-live-owner-tokens result)))
    (is (seq (:mismatched-views result)))))

(deftest rejects-a-live-unresolved-registry-conflict
  (let [owners [(owner "a" [(registration nil 0 1)] [])
                (owner "b" [(registration nil 0 2)] [])]
        registry (assoc-in (empty-registry) ["root" 0] "b")
        history [(snapshot-op 1 "n1" owners registry (empty-pg))
                 (snapshot-op 2 "n2" [] registry (empty-pg))
                 (snapshot-op 3 "n3" [] registry (empty-pg))]
        result (model/analyze test-map history)]
    (is (false? (:valid? result)))
    (is (= {["root" 0] #{"a" "b"}} (:live-registry-conflicts result)))))

(deftest rejects-an-unexpected-owner-death-even-after-cleanup
  (let [result (model/analyze
                 test-map
                 [(with-unexpected-death
                    (snapshot-op 1 "n1" [] (empty-registry) (empty-pg))
                    "lost-owner")
                  (snapshot-op 2 "n2" [] (empty-registry) (empty-pg))
                  (snapshot-op 3 "n3" [] (empty-registry) (empty-pg))])]
    (is (false? (:valid? result)))
    (is (= #{{:token "lost-owner", :reason ":boom"}}
           (:unexpected-owner-deaths result)))))

(deftest rejects-terminal-state-which-keeps-changing
  (let [stale-registry (assoc-in (empty-registry) ["root" 0] "stale")
        history [(snapshot-op 1 "n1" [] stale-registry (empty-pg))
                 (snapshot-op 2 "n2" [] (empty-registry) (empty-pg))
                 (snapshot-op 3 "n3" [] (empty-registry) (empty-pg))
                 (snapshot-op 4 "n1" [] (empty-registry) (empty-pg))
                 (snapshot-op 5 "n2" [] (empty-registry) (empty-pg))
                 (snapshot-op 6 "n3" [] (empty-registry) (empty-pg))]
        result (model/analyze
                 (assoc test-map :terminal-snapshots-per-node 2)
                 history)]
    (is (false? (:valid? result)))
    (is (contains? (:unstable-terminal-observations result) "n1"))))

(deftest rejects-a-node-without-all-control-plane-peers
  (let [history [(assoc-in (snapshot-op 1 "n1" [] (empty-registry) (empty-pg))
                           [:value :peers]
                           ["group@n2"])
                 (snapshot-op 2 "n2" [] (empty-registry) (empty-pg))
                 (snapshot-op 3 "n3" [] (empty-registry) (empty-pg))]
        result (model/analyze test-map history)]
    (is (false? (:valid? result)))
    (is (= #{"group@n2" "group@n3"}
           (get-in result [:peer-mismatches "n1" :expected])))))

(deftest rejects-a-run-which-did-not-exercise-required-repair-paths
  (let [history [(snapshot-op 1 "n1" [] (empty-registry) (empty-pg))
                 (snapshot-op 2 "n2" [] (empty-registry) (empty-pg))
                 (snapshot-op 3 "n3" [] (empty-registry) (empty-pg))]
        result (model/analyze
                 (assoc test-map
                        :required-transport-events
                        model/default-required-transport-events)
                 history)]
    (is (false? (:valid? result)))
    (is (= model/default-required-transport-events
           (:missing-transport-events result)))))

(deftest accepts-the-transport-event-names-emitted-by-the-live-nodes
  (let [events {:delta-batch 1
                :snapshot-chunk 2
                :multi-chunk-snapshot 1
                :registry-conflict-death 1}
        history [(assoc-in (with-conflict-evidence (snapshot-op 1 "n1" [] (empty-registry) (empty-pg)))
                           [:value :transport-events]
                           events)
                 (snapshot-op 2 "n2" [] (empty-registry) (empty-pg))
                 (snapshot-op 3 "n3" [] (empty-registry) (empty-pg))]
        result (model/analyze
                 (dissoc test-map :required-transport-events)
                 history)]
    (is (:valid? result))
    (is (empty? (:missing-transport-events result)))))

(deftest rejects-a-profile-which-never-repairs-a-multi-record-delta-run
  (let [single-record-events {:delta-batch 3
                              :snapshot-chunk 2
                              :multi-chunk-snapshot 1
                              :registry-conflict-death 1
                              :delta-run-records-peak 1}
        with-events #(assoc-in % [:value :transport-events] single-record-events)
        history [(with-events (snapshot-op 1 "n1" [] (empty-registry) (empty-pg)))
                 (with-events (snapshot-op 2 "n2" [] (empty-registry) (empty-pg)))
                 (with-events (snapshot-op 3 "n3" [] (empty-registry) (empty-pg)))]
        result (model/analyze (assoc test-map :min-delta-run-records 2) history)]
    (is (false? (:valid? result)))
    (is (= 1 (:delta-run-records-peak result)))
    (is (= 2 (:min-delta-run-records result)))))

(deftest accepts-a-profile-which-repairs-a-multi-record-delta-run
  (let [events {:delta-batch 1
                :snapshot-chunk 1
                :multi-chunk-snapshot 1
                :registry-conflict-death 1
                :delta-run-records-peak 8}
        history [(assoc-in (snapshot-op 1 "n1" [] (empty-registry) (empty-pg))
                           [:value :transport-events]
                           events)
                 (snapshot-op 2 "n2" [] (empty-registry) (empty-pg))
                 (snapshot-op 3 "n3" [] (empty-registry) (empty-pg))]
        result (model/analyze (assoc test-map :min-delta-run-records 2) history)]
    (is (:valid? result))
    (is (= 8 (:delta-run-records-peak result)))))

(deftest rejects-internal-corruption-or-leftover-snapshot-staging
  (let [bad (-> (snapshot-op 1 "n1" [] (empty-registry) (empty-pg))
                (assoc-in [:value :internal :healthy] false)
                (assoc-in [:value :internal :snapshot-staging-count] 1)
                (assoc-in [:value :internal :errors] ["broken index"]))
        result (model/analyze
                 test-map
                 [bad
                  (snapshot-op 2 "n2" [] (empty-registry) (empty-pg))
                  (snapshot-op 3 "n3" [] (empty-registry) (empty-pg))])]
    (is (false? (:valid? result)))
    (is (= 1 (get-in result [:internal-invariant-errors "n1"
                             :snapshot-staging-count])))))

(deftest rejects-the-wrong-transport-profile
  (let [wrong (assoc-in (snapshot-op 1 "n1" [] (empty-registry) (empty-pg))
                        [:value :transport-profile]
                        :tcp)
        result (model/analyze
                 test-map
                 [wrong
                  (snapshot-op 2 "n2" [] (empty-registry) (empty-pg))
                  (snapshot-op 3 "n3" [] (empty-registry) (empty-pg))])]
    (is (false? (:valid? result)))
    (is (= {"n1" :tcp} (:transport-profile-mismatches result)))))

(deftest rejects-a-blocking-group-operation
  (let [base [(snapshot-op 1 "n1" [] (empty-registry) (empty-pg))
              (snapshot-op 2 "n2" [] (empty-registry) (empty-pg))
              (snapshot-op 3 "n3" [] (empty-registry) (empty-pg))]
        slow {:index 4
              :process 0
              :type :ok
              :f :register
              :value {:response {:latency-us 2500000}}}
        result (model/analyze test-map (conj base slow))]
    (is (false? (:valid? result)))
    (is (= 2500.0 (:max-group-operation-latency-ms result)))))
