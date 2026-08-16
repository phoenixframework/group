(ns group.jepsen.model-test
  (:require [clojure.test :refer :all]
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
        history [(assoc-in (snapshot-op 1 "n1" [] (empty-registry) (empty-pg))
                           [:value :transport-events]
                           events)
                 (snapshot-op 2 "n2" [] (empty-registry) (empty-pg))
                 (snapshot-op 3 "n3" [] (empty-registry) (empty-pg))]
        result (model/analyze
                 (dissoc test-map :required-transport-events)
                 history)]
    (is (:valid? result))
    (is (empty? (:missing-transport-events result)))))

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
