(ns group.jepsen.db
  (:require [group.jepsen.client :as group-client]
            [group.jepsen.docker :as docker]
            [jepsen.db :as db]))

(defrecord DockerDB []
  db/DB
  (setup! [_this test node]
    (docker/heal! (:nodes test))
    (docker/restart! node)
    (docker/reset-oracle! node)
    (group-client/wait-ready! node (count (:nodes test))))

  (teardown! [_this test _node]
    (docker/heal! (:nodes test)))

  db/Kill
  (kill! [_this _test node]
    (docker/stop! node))

  (start! [_this _test node]
    (docker/start! node)
    (group-client/wait-listening! node))

  db/LogFiles
  (log-files [_this _test _node] []))

(defn db [], (DockerDB.))
