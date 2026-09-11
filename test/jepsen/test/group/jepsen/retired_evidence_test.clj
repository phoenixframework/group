(ns group.jepsen.retired-evidence-test
  (:require [clojure.test :refer :all]
            [group.jepsen.docker :as docker]
            [group.jepsen.client :as client]
            [group.jepsen.db :as group-db]
            [group.jepsen.nemesis :as group-nemesis]
            [jepsen.db :as db]
            [jepsen.nemesis :as nemesis]))

(deftest collector-reads-stopped-container-through-command-boundary
  (let [calls (atom [])]
    (with-redefs [docker/docker!
                  (fn [& args]
                    (swap! calls conj args)
                    (is (= 10000 docker/*command-timeout-ms*))
                    (spit (last args) "n1/boot/owner/1\t:boom\n"))
                  docker/decode-conflict-evidence! (constantly [])]
      (is (= {:node "n1" :unexpected-deaths [{:token "n1/boot/owner/1" :reason ":boom"}]
              :conflict-evidence []}
             (docker/retired-evidence! "n1")))
      (is (= ["cp" "group-jepsen-n1:/tmp/group-jepsen-unexpected-deaths"]
             (vec (take 2 (first @calls))))))))

(deftest collector-distinguishes-empty-evidence-from-loss
  (doseq [contents ["" "bad\n" "token\t:boom"]]
    (with-redefs [docker/docker! (fn [& args] (spit (last args) contents))
                  docker/decode-conflict-evidence! (constantly [])]
      (if (= "" contents)
        (is (= [] (:unexpected-deaths (docker/retired-evidence! "n1"))))
        (is (thrown? Exception (docker/retired-evidence! "n1"))))))
  (with-redefs [docker/docker! (fn [& _] (throw (ex-info "missing file" {:exit 1})))]
    (is (thrown? Exception (docker/retired-evidence! "n1")))))

(deftest workload-reset-initializes-empty-evidence-before-mutations
  (with-redefs [docker/exec-sh!
                (fn [node script]
                  (is (= "n1" node))
                  (is (re-find #": > /tmp/group-jepsen-unexpected-deaths" script)))]
    (docker/reset-oracle! "n1")))

(deftest workload-reset-clears-the-running-conflict-recorder
  (let [calls (atom [])]
    (with-redefs [docker/heal! (fn [_])
                  docker/restart! #(swap! calls conj [:restart %])
                  docker/reset-oracle! #(swap! calls conj [:disk-reset %])
                  client/wait-listening! #(swap! calls conj [:listening %])
                  client/request! (fn [node fields]
                                    (swap! calls conj [node fields])
                                    {:status :ok})
                  client/wait-ready! (fn [node _] (swap! calls conj [:ready node]))]
      (dotimes [_ 2]
        (db/setup! (group-db/db) {:nodes ["n1"]} "n1"))
      (is (= (vec (mapcat identity (repeat 2 [[:restart "n1"] [:disk-reset "n1"]
                                             [:listening "n1"]
                                             ["n1" ["reset-conflict-evidence"]]
                                             [:ready "n1"]])))
             @calls)))))

(deftest conflict-archive-decode-fails-closed
  (doseq [contents ["not-base64\n" "truncated"]]
    (with-redefs [docker/docker!
                  (fn [& args]
                    (spit (last args)
                          (if (.contains (second args) "conflict-evidence") contents "")))]
      (is (thrown? Exception (docker/retired-evidence! "n1"))))))

(deftest retirement-captures-after-stop-even-if-node-was-already-unavailable
  (doseq [running? [true false]]
    (let [calls (atom [])
          database (reify db/Process
                     (start! [_ _ _])
                     (kill! [_ _ node] (swap! calls conj [:kill node])))
          n (group-nemesis/->RetirementNemesis database (atom nil))]
      (with-redefs [docker/running? (constantly running?)
                    docker/retired-evidence! (fn [node]
                                               (swap! calls conj [:collect node])
                                               {:node node :unexpected-deaths []})]
        (is (= {:node "n1" :unexpected-deaths []}
               (get-in (nemesis/invoke! n {:nodes ["n1"]} {:f :retire})
                       [:value :lifecycle-evidence])))
        (is (= (if running? [[:kill "n1"] [:collect "n1"]] [[:collect "n1"]])
               @calls))))))

(deftest retirement-preserves-collector-failure-in-history
  (with-redefs [docker/running? (constantly false)
                docker/retired-evidence! (fn [_] (throw (ex-info "timeout" {})))]
    (let [n (group-nemesis/->RetirementNemesis nil (atom nil))
          op (nemesis/invoke! n {:nodes ["n1"]} {:f :retire})]
      (is (= "timeout" (get-in op [:value :evidence-error :message]))))))

(deftest command-timeout-is-bounded
  (binding [docker/*command-timeout-ms* 20]
    (is (thrown-with-msg? Exception #"timed out" (docker/shell! "sleep" "10")))))
