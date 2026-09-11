(ns group.jepsen.streams-test
  (:require [clojure.test :refer :all]
            [group.jepsen.model :as model]
            [group.jepsen.model-test :as fixtures]
            [group.jepsen.streams :as streams]))

(defn evidence [origin generation epochs heads cursors]
  {:origin origin :generation generation :shards 2 :epochs epochs
   :heads (vec (for [[cluster epoch] epochs, shard (range 2)]
                 {:stream {:group "jepsen_group" :origin origin :generation generation
                           :shard shard :cluster cluster :epoch epoch}
                  :head (get heads [cluster shard] 0)
                  :applied (get heads [cluster shard] 0)}))
   :cursors cursors})

(defn cursor [evidence cluster shard position]
  {:stream (:stream (first (filter #(and (= cluster (get-in % [:stream :cluster]))
                                         (= shard (get-in % [:stream :shard])))
                                   (:heads evidence))))
   :lane shard :position position})

(def origin (evidence "a" "g1" {"root" "g1" "red" "e1"} {["root" 0] 1} []))
(def receiver (evidence "b" "g2" {"root" "g2" "red" "e2"} {}
                        [(cursor origin "root" 0 1)]))
(def survivor (evidence "c" "g3" {"root" "g3"} {}
                        [(cursor origin "root" 0 1)]))
(def baseline {"a" {:streams origin} "b" {:streams receiver} "c" {:streams survivor}})

(deftest accepts-exact-and-pristine-streams
  (is (empty? (streams/errors baseline)))
  (is (empty? (streams/errors
                (update-in baseline ["b" :streams :cursors]
                           conj (cursor origin "red" 1 0))))))

(deftest rejects-out-of-range-missing-and-misplaced-cursors
  (doseq [position [101 0 -1 "1"]]
    (is (seq (streams/errors
               (assoc-in baseline ["b" :streams :cursors 0 :position] position)))))
  (is (seq (streams/errors (assoc-in baseline ["b" :streams :cursors] [])))))

(deftest rejects-every-stale-stream-dimension
  (doseq [[field wrong] [[:group "other"] [:origin "retired"] [:generation "old"]
                         [:epoch "closed"] [:cluster "blue"] [:shard 1]]]
    (is (seq (streams/errors
               (assoc-in baseline ["b" :streams :cursors 0 :stream field] wrong))))))

(deftest requires-complete-origin-evidence
  (is (seq (streams/errors (update baseline "a" dissoc :streams))))
  (is (seq (streams/errors (assoc-in baseline ["a" :streams :heads] []))))
  (is (seq (streams/errors (assoc-in baseline ["a" :streams :heads 0 :applied] -1)))))

(deftest closed-and-restarted-origins-cannot-retain-cursors
  (let [with-red (update-in baseline ["b" :streams :cursors]
                           conj (cursor origin "red" 0 0))
        closed (evidence "a" "g1" {"root" "g1"} {["root" 0] 1} [])]
    (is (empty? (streams/errors with-red)))
    (is (seq (streams/errors (assoc-in with-red ["a" :streams] closed))))
    (is (empty? (streams/errors (assoc-in baseline ["a" :streams] closed)))))
  (let [retired (dissoc baseline "a")]
    (is (seq (streams/errors retired)))
    (is (empty? (streams/errors
                  (into {} (map (fn [[node snapshot]]
                                  [node (assoc-in snapshot [:streams :cursors] [])]))
                        retired)))))
  (let [restarted (assoc-in baseline ["a" :streams]
                           (evidence "a" "new" {"root" "new"} {} []))]
    (is (seq (streams/errors restarted)))
    (is (empty? (streams/errors
                  (-> restarted
                      (assoc-in ["b" :streams :cursors] [])
                      (assoc-in ["c" :streams :cursors] [])))))))

(deftest terminal-checker-requires-stable-stream-positions
  (let [history (mapv #(fixtures/snapshot-op % (str "n" %) []
                                            (fixtures/empty-registry) (fixtures/empty-pg))
                      [1 2 3])
        changed (assoc-in history [1 :value :streams :heads 0 :head] 1)
        changed (assoc-in changed [1 :value :streams :heads 0 :applied] 1)]
    (is (:valid? (model/analyze fixtures/test-map history)))
    (is (false? (:valid? (model/analyze fixtures/test-map changed))))
    (is (seq (:stream-position-errors (model/analyze fixtures/test-map changed))))
    (let [two-rounds (concat history (map #(update % :index + 3) changed))
          result (model/analyze (assoc fixtures/test-map :terminal-snapshots-per-node 2)
                                two-rounds)]
      (is (contains? (:unstable-terminal-observations result) "n2")))))
