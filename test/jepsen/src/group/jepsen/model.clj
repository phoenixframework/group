(ns group.jepsen.model
  (:require [clojure.set :as set]
            [jepsen.checker :as checker]
            [jepsen.history :as history]))

(defn successful-snapshots [history]
  (->> history
       (remove history/invoke?)
       (filter #(and (= :snapshot (:f %)) (= :ok (:type %))))
       (sort-by :index)))

(defn snapshots-by-node [history]
  (reduce (fn [snapshots op]
            (update snapshots (get-in op [:value :node]) (fnil conj []) (:value op)))
          {}
          (successful-snapshots history)))

(defn latest-snapshots [history]
  (->> (successful-snapshots history)
       (reduce (fn [snapshots op]
                 (assoc snapshots (get-in op [:value :node]) (:value op)))
               {})))

(defn spaces [test]
  (cons "root" (:clusters test)))

(defn empty-view [test empty-value]
  (into {}
        (for [cluster (spaces test)]
          [cluster (zipmap (range (:key-count test)) (repeat empty-value))])))

(defn owner-entries [owner field]
  (or (get owner field) []))

(defn expected-state [test snapshots]
  (let [owners (->> snapshots vals (mapcat :owners) (map (juxt :token identity)) (into {}))
        registry-candidates
        (reduce (fn [by-key [_ owner]]
                  (reduce (fn [entries {:keys [cluster key]}]
                            (update entries [(or cluster "root") key]
                                    (fnil conj #{}) (:token owner)))
                          by-key
                          (owner-entries owner :registrations)))
                {}
                owners)
        conflicts (into {} (filter (comp #(< 1 %) count val) registry-candidates))
        registry
        (reduce (fn [view [[cluster key] tokens]]
                  (assoc-in view [cluster key] (first tokens)))
                (empty-view test nil)
                registry-candidates)
        pg
        (reduce (fn [view [_ owner]]
                  (reduce (fn [entries {:keys [cluster key]}]
                            (update-in entries [(or cluster "root") key]
                                       (fnil conj #{}) (:token owner)))
                          view
                          (owner-entries owner :memberships)))
                (empty-view test #{})
                owners)]
    {:owners owners, :registry registry, :pg pg, :conflicts conflicts}))

(defn normalize-view [test snapshot]
  {:registry
   (into {}
         (for [cluster (spaces test)]
           [cluster
            (into {}
                  (for [key (range (:key-count test))]
                    [key (get-in snapshot [:registry cluster key])]))]))
   :pg
   (into {}
         (for [cluster (spaces test)]
           [cluster
            (into {}
                  (for [key (range (:key-count test))]
                    [key (set (get-in snapshot [:pg cluster key]))]))]))})

(defn stable-internal [snapshot]
  (select-keys (:internal snapshot)
               [:healthy :errors :snapshot-staging-count :oplog-entries]))

(defn snapshot-fingerprint [test snapshot]
  {:owners (set (:owners snapshot))
   :peers (set (:peers snapshot))
   :unexpected-deaths (set (:unexpected-deaths snapshot))
   :view (normalize-view test snapshot)
   :internal (stable-internal snapshot)})

(defn operation-latencies [history]
  (->> history
       (remove history/invoke?)
       (keep #(get-in % [:value :response :latency-us]))))

(defn analyze [test history]
  (let [observations (snapshots-by-node history)
        snapshots (latest-snapshots history)
        required-nodes (set (map name (or (:terminal-nodes test) (:nodes test))))
        relevant-observations (select-keys observations required-nodes)
        relevant-snapshots (select-keys snapshots required-nodes)
        missing-nodes (set/difference required-nodes (set (keys relevant-snapshots)))
        minimum-observations (get test :terminal-snapshots-per-node 2)
        insufficient-observations
        (into {}
              (keep (fn [node]
                      (let [observation-count (count (get relevant-observations node))]
                        (when (< observation-count minimum-observations)
                          [node observation-count]))))
              required-nodes)
        unstable-observations
        (into {}
              (keep (fn [[node node-observations]]
                      (let [fingerprints
                            (set (map #(snapshot-fingerprint test %)
                                      node-observations))]
                        (when (< 1 (count fingerprints))
                          [node fingerprints]))))
              relevant-observations)
        expected (expected-state test relevant-snapshots)
        expected-view (select-keys expected [:registry :pg])
        views (into {} (map (fn [[node snapshot]]
                              [node (normalize-view test snapshot)]))
                    relevant-snapshots)
        mismatches (into {} (remove (comp #(= expected-view %) val) views))
        peer-mismatches
        (into {}
              (keep (fn [[node snapshot]]
                      (let [expected-peers (->> required-nodes
                                                (remove #(= node %))
                                                (map #(str "group@" %))
                                                set)
                            actual-peers (set (:peers snapshot))]
                        (when (not= expected-peers actual-peers)
                          [node {:expected expected-peers, :actual actual-peers}]))))
              relevant-snapshots)
        transport-events
        (reduce #(merge-with + %1 %2)
                {}
                (map #(or (:transport-events %) {}) (vals relevant-snapshots)))
        required-transport-events
        (get test :required-transport-events
             #{:delta-batch :snapshot-chunk :multi-chunk-snapshot-chunk
               :registry-conflict-death})
        missing-transport-events
        (set (remove #(pos? (get transport-events % 0)) required-transport-events))
        expected-profile (keyword (:transport test))
        transport-profile-mismatches
        (into {}
              (keep (fn [[node snapshot]]
                      (when (not= expected-profile (:transport-profile snapshot))
                        [node (:transport-profile snapshot)])))
              relevant-snapshots)
        internal-errors
        (into {}
              (keep (fn [[node snapshot]]
                      (let [internal (:internal snapshot)]
                        (when (or (not= true (:healthy internal))
                                  (seq (:errors internal))
                                  (not= 0 (:snapshot-staging-count internal)))
                          [node internal]))))
              relevant-snapshots)
        unexpected-deaths (->> relevant-snapshots vals (mapcat :unexpected-deaths) set)
        live-tokens (set (keys (:owners expected)))
        actual-tokens (->> views
                           vals
                           (mapcat (fn [{:keys [registry pg]}]
                                     (concat
                                       (->> registry vals (mapcat vals) (remove nil?))
                                       (->> pg vals (mapcat vals) (mapcat identity)))))
                           set)
        expected-tokens
        (set/union
          (->> (:registry expected) vals (mapcat vals) (remove nil?) set)
          (->> (:pg expected) vals (mapcat vals) (mapcat identity) set))
        orphaned (set/difference actual-tokens live-tokens)
        missing-live (set/difference expected-tokens actual-tokens)
        latencies (operation-latencies history)
        max-latency-us (if (seq latencies) (apply max latencies) 0)
        latency-limit-us (* 1000 (get test :max-operation-latency-ms 2000))
        latency-violation? (> max-latency-us latency-limit-us)
        valid? (and (empty? missing-nodes)
                    (empty? insufficient-observations)
                    (empty? unstable-observations)
                    (empty? peer-mismatches)
                    (empty? missing-transport-events)
                    (empty? transport-profile-mismatches)
                    (empty? internal-errors)
                    (empty? (:conflicts expected))
                    (empty? mismatches)
                    (empty? unexpected-deaths)
                    (empty? orphaned)
                    (empty? missing-live)
                    (not latency-violation?))]
    {:valid? valid?
     :snapshots (set (keys relevant-snapshots))
     :missing-nodes missing-nodes
     :insufficient-terminal-observations insufficient-observations
     :unstable-terminal-observations unstable-observations
     :peer-mismatches peer-mismatches
     :transport-events transport-events
     :missing-transport-events missing-transport-events
     :transport-profile-mismatches transport-profile-mismatches
     :internal-invariant-errors internal-errors
     :max-group-operation-latency-ms (/ max-latency-us 1000.0)
     :group-operation-latency-limit-ms (/ latency-limit-us 1000.0)
     :live-owner-count (count live-tokens)
     :live-registry-conflicts (:conflicts expected)
     :mismatched-views mismatches
     :unexpected-owner-deaths unexpected-deaths
     :orphaned-owner-tokens orphaned
     :missing-live-owner-tokens missing-live
     :expected expected-view}))

(defrecord LifecycleChecker []
  checker/Checker
  (check [_this test history _opts]
    (analyze test history)))

(defn checker [], (LifecycleChecker.))
