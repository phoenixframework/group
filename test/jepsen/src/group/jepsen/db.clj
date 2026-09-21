(ns group.jepsen.db
  (:require [group.jepsen.client :as group-client]
            [group.jepsen.docker :as docker]
            [jepsen.db :as db]))

(defrecord DockerDB []
  db/DB
  (setup! [_this test node]
    (docker/restart! node)
    ;; Jepsen sets up nodes concurrently. Never exec firewall commands on a
    ;; sibling while its setup thread may be restarting its container.
    (docker/heal! [node])
    (docker/reset-oracle! node)
    (group-client/wait-listening! node)
    (let [response (group-client/request! node ["reset-conflict-evidence"])]
      (when-not (= :ok (:status response))
        (throw (ex-info "conflict oracle reset failed" {:node node :response response}))))
    (group-client/wait-ready! node (count (:nodes test))))

  (teardown! [_this _test node]
    (docker/heal! [node]))

  db/Kill
  (kill! [_this _test node]
    (docker/stop! node))

  (start! [_this _test node]
    (docker/start! node)
    (group-client/wait-listening! node))

  db/LogFiles
  (log-files [_this _test _node] []))

(defn db [], (DockerDB.))
