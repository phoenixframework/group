(ns group.jepsen.streams
  (:require [clojure.set :as set]))

(defn natural? [n] (and (integer? n) (<= 0 n)))

(defn evidence-valid? [{:keys [origin generation shards epochs heads cursors]}]
  (and (string? origin) (string? generation)
       (integer? shards) (< 0 shards)
       (map? epochs) (= generation (get epochs "root"))
       (every? string? (keys epochs)) (every? string? (vals epochs))
       (sequential? heads) (sequential? cursors)
       (= (count heads) (count (set (map :stream heads))))
       (= (count cursors) (count (set (map :stream cursors))))
       (= (set (map :stream heads))
          (set (for [shard (range shards), [cluster epoch] epochs]
                 {:group "jepsen_group" :origin origin :generation generation
                  :shard shard :cluster cluster :epoch epoch})))
       (every? #(and (natural? (:head %)) (= (:head %) (:applied %))) heads)
       (every? #(and (natural? (:position %))
                     (natural? (:lane %)) (< (:lane %) shards)
                     (= (:lane %) (get-in % [:stream :shard]))) cursors)))

(defn errors
  "At stable quiescence every admitted remote stream equals its origin head.
   Head zero permits an absent cursor or an explicit zero admission marker.
   Admission comes from both origins' current active epochs, not from the
   receiver's existing cursor set (which could itself be missing or stale)."
  [snapshots]
  (let [evidence (into {} (map (fn [[node snapshot]] [node (:streams snapshot)])) snapshots)
        invalid (into {} (remove (comp evidence-valid? val)) evidence)
        origins (map :origin (vals evidence))]
    (if (or (seq invalid) (not= (count origins) (count (set origins))))
      {:invalid-evidence invalid :duplicate-origins (not= (count origins) (count (set origins)))}
      (into {}
            (keep
              (fn [[node receiver]]
                (let [expected
                      (into {}
                            (for [[other sender] evidence
                                  :when (not= other node)
                                  {:keys [stream head]} (:heads sender)
                                  :when (contains? (:epochs receiver) (:cluster stream))]
                              [stream head]))
                      actual (into {} (map (juxt :stream :position)) (:cursors receiver))
                      mismatches
                      (into {}
                            (keep (fn [stream]
                                    (let [head (get expected stream)
                                          cursor (get actual stream 0)]
                                      (when (not= head cursor)
                                        [stream {:head head
                                                 :cursor (get actual stream :missing)}]))))
                            (set/union (set (keys expected)) (set (keys actual))))
                      shard-counts (set (map :shards (vals evidence)))]
                  (when (or (seq mismatches) (< 1 (count shard-counts)))
                    [node {:positions mismatches :shard-counts shard-counts}]))))
            evidence))))
