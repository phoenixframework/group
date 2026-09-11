(ns group.jepsen.cursor-capture-test
  (:require [clojure.edn :as edn]
            [clojure.java.shell :as shell]
            [clojure.test :refer :all]
            [group.jepsen.streams :as streams]))

(deftest ^:capture compares-real-three-node-cursors-with-independent-origin-heads
  (let [output (java.io.File/createTempFile "group-cursors-" ".edn")]
    (try
      (let [run (shell/sh "env" "ERL_FLAGS=+S 2:2" "MIX_ENV=test" "mix" "run"
                          "test/jepsen/cursor_capture.exs" (.getPath output)
                          :dir "../..")]
        (is (= 0 (:exit run)) (str (:out run) (:err run)))
        (when (zero? (:exit run))
          (let [captures (edn/read-string (slurp output))]
            (doseq [kind [:pristine :healthy :zero :closed :restarted :retired]]
              (is (empty? (streams/errors (get captures kind))) (str kind))
              (is (every? #(true? (get-in % [:internal :healthy])) (vals (get captures kind)))))
            (doseq [snapshots (:corruptions captures)]
              (is (every? #(true? (get-in % [:internal :healthy])) (vals snapshots)))
              (is (seq (streams/errors snapshots)))))))
      (finally (.delete output)))))
