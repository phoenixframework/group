(ns group.jepsen.db-test
  (:require [clojure.test :refer :all]
            [group.jepsen.client :as client]
            [group.jepsen.db :as group-db]
            [group.jepsen.docker :as docker]
            [jepsen.db :as db])
  (:import (java.util.concurrent CountDownLatch TimeUnit)))

(deftest concurrent-setup-heals-only-its-own-restarted-container
  (let [nodes ["n1" "n2" "n3"]
        test {:nodes nodes}
        database (group-db/db)
        restarting (CountDownLatch. 3)
        running (atom #{})
        healed (atom [])]
    (with-redefs [docker/restart!
                  (fn [node]
                    (.countDown restarting)
                    (when-not (.await restarting 5 TimeUnit/SECONDS)
                      (throw (ex-info "setups did not restart concurrently" {})))
                    (swap! running conj node))
                  docker/heal!
                  (fn [targets]
                    (is (= 1 (count targets)))
                    (is (every? @running targets))
                    (swap! healed into targets))
                  docker/reset-oracle! (fn [_])
                  client/wait-listening! (fn [_])
                  client/request! (fn [_ _] {:status :ok})
                  client/wait-ready! (fn [_ size] (is (= 3 size)))]
      (let [setups (mapv #(future (db/setup! database test %)) nodes)]
        (try
          (doseq [setup setups]
            (is (not= ::timeout (deref setup 10000 ::timeout))))
          (is (= (sort nodes) (sort @healed)))
          (finally
            (doseq [setup setups]
              (future-cancel setup))))))))

(deftest teardown-heals-each-container-once
  (let [nodes ["n1" "n2" "n3"]
        calls (atom [])]
    (with-redefs [docker/heal! #(swap! calls conj %)]
      (doseq [node nodes]
        (db/teardown! (group-db/db) {:nodes nodes} node))
      (is (= [["n1"] ["n2"] ["n3"]] @calls)))))

(deftest setup-does-not-hide-firewall-failures
  (with-redefs [docker/restart! (fn [_])
                docker/heal! (fn [_] (throw (ex-info "firewall failed" {:exit 1})))]
    (is (thrown-with-msg? Exception #"firewall failed"
                         (db/setup! (group-db/db) {:nodes ["n1"]} "n1")))))
