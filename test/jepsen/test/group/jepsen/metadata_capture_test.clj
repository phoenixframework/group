(ns group.jepsen.metadata-capture-test
  (:require [clojure.edn :as edn]
            [clojure.java.shell :as shell]
            [clojure.test :refer :all]
            [group.jepsen.model :as model]))

(def capture-test
  {:nodes ["n1"] :key-count 1 :clusters [] :transport "distribution"
   :terminal-snapshots-per-node 1 :required-transport-events #{}})

(defn analyze-snapshot [snapshot]
  (model/analyze capture-test
                 [{:index 0 :process 0 :type :ok :f :snapshot :value snapshot}]))

(deftest ^:capture captures-acknowledged-metadata-through-the-real-edn-boundary
  (let [output (java.io.File/createTempFile "group-metadata-" ".edn")]
    (try
      (let [run (shell/sh "env" "ERL_FLAGS=+S 2:2" "MIX_ENV=test" "mix" "run"
                          "test/jepsen/metadata_capture.exs" (.getPath output)
                          :dir "../..")]
        (is (= 0 (:exit run)) (str (:out run) (:err run)))
        (when (zero? (:exit run))
          (let [{:keys [healthy corruptions]} (edn/read-string (slurp output))]
            (is (:valid? (analyze-snapshot healthy)))
            (doseq [snapshot corruptions]
              (let [result (analyze-snapshot snapshot)]
                (is (true? (get-in snapshot [:internal :healthy])))
                (is (false? (:valid? result)))
                (is (seq (:mismatched-views result))))))))
      (finally (.delete output)))))
