(ns group.jepsen.core
  (:gen-class)
  (:require [group.jepsen.client :as group-client]
            [group.jepsen.db :as group-db]
            [group.jepsen.model :as model]
            [group.jepsen.nemesis :as group-nemesis]
            [jepsen.cli :as cli]
            [jepsen.generator :as gen]
            [jepsen.os :as os]
            [jepsen.tests :as tests]))

(def clusters ["red" "blue"])

(defn mutation [revision key-count owner-count]
  (let [key (rand-int key-count)
        owner (rand-int owner-count)
        cluster (rand-nth [nil nil nil "red" "blue"])
        rev #(swap! revision inc)]
    (case (long (rand-int 16))
      0 {:f :kill, :value {:owner owner}}
      1 {:f :unregister, :value {:owner owner, :cluster cluster, :key key}}
      2 {:f :leave, :value {:owner owner, :cluster cluster, :key key}}
      3 {:f :cluster-disconnect, :value {:cluster (rand-nth clusters)}}
      4 {:f :cluster-connect, :value {:cluster (rand-nth clusters)}}
      5 {:f :join, :value {:owner owner, :cluster cluster, :key key, :revision (rev)}}
      6 {:f :join, :value {:owner owner, :cluster cluster, :key key, :revision (rev)}}
      7 {:f :join, :value {:owner owner, :cluster cluster, :key key, :revision (rev)}}
      8 {:f :join, :value {:owner owner, :cluster cluster, :key key, :revision (rev)}}
      {:f :register, :value {:owner owner, :cluster cluster, :key key, :revision (rev)}})))

(defn fault-cycle [fault-interval]
  (cycle [(gen/sleep fault-interval)
          {:type :info, :f :replica-partition-start}
          (gen/sleep fault-interval)
          {:type :info, :f :replica-reset}
          (gen/sleep fault-interval)
          {:type :info, :f :kill-node}
          (gen/sleep fault-interval)
          {:type :info, :f :restart-node}
          (gen/sleep fault-interval)
          {:type :info, :f :replica-partition-stop}
          (gen/sleep fault-interval)
          {:type :info, :f :partition-start}
          (gen/sleep fault-interval)
          {:type :info, :f :partition-stop}]))

(defn targeted [target f value]
  {:f f, :value (assoc value :target target)})

(defn prelude []
  [(gen/log "Opening a three-way replica partition for deterministic conflict and epoch fencing")
   (gen/nemesis {:type :info, :f :replica-partition-start, :value {:shape :all}})
   (gen/sleep 0.25)
   (gen/clients
     (gen/once
       (targeted :n1 :register
                 {:owner "epoch-old", :cluster "red", :key 0, :revision 1000})))
   (gen/clients
     (gen/once (targeted :n1 :cluster-disconnect {:cluster "red"})))
   (gen/clients
     (gen/once (targeted :n1 :cluster-connect {:cluster "red"})))
   (gen/clients
     (gen/once
       (targeted :n1 :register
                 {:owner "epoch-new", :cluster "red", :key 0, :revision 1001})))
   (gen/log "Creating three independent claims for one root registry key")
   (gen/clients
     [(targeted :n1 :register {:owner "triple-n1", :cluster nil, :key 0, :revision 2001})
      (targeted :n2 :register {:owner "triple-n2", :cluster nil, :key 0, :revision 2002})
      (targeted :n3 :register {:owner "triple-n3", :cluster nil, :key 0, :revision 2003})])
   (gen/sleep 0.25)
   (gen/nemesis {:type :info, :f :replica-partition-stop})
   (gen/sleep 1)
   (gen/log "Restarting n2 after conflict resolution to prove durable qualification evidence")
   (gen/nemesis {:type :info, :f :kill-node, :value {:node :n2}})
   (gen/nemesis {:type :info, :f :restart-node})
   (gen/sleep 1)])

(defn terminal-snapshot [opts]
  {:f :snapshot
   :value {:key-count (:key-count opts)
           :clusters clusters
           :retired-nodes (:retired-nodes opts)}})

(defn snapshot-round [opts]
  (let [read (terminal-snapshot opts)
        permanent? (= "permanent" (:scenario opts))]
    (gen/clients
      (gen/each-thread
        (if permanent?
          (gen/once read)
          (gen/until-ok (repeat read)))))))

(defn terminal-phases [opts]
  (let [permanent? (= "permanent" (:scenario opts))
        corruption (keyword (:corruption opts))]
    (cond->
      [(gen/log "Healing every fault and restarting transiently killed nodes")
       (gen/nemesis {:type :info, :f :restart-node})
       (gen/nemesis {:type :info, :f :partition-stop})
       (gen/nemesis {:type :info, :f :replica-partition-stop})
       (gen/clients
         (gen/each-thread
           (gen/until-ok (repeat {:f :connect-all, :value {}}))))
       (gen/sleep (:recovery-time opts))]
      permanent?
      (conj (gen/log "Retiring n1 permanently and waiting for complete eviction")
            (gen/nemesis {:type :info, :f :retire-node, :value {:node :n1}})
            (gen/sleep (:recovery-time opts)))

      (not= :none corruption)
      (conj (gen/log "Injecting a checker-qualification corruption")
            (gen/clients
              (gen/once
                (targeted (first (:terminal-nodes opts)) :corrupt {:mode corruption}))))

      true
      (conj (gen/log "Collecting first terminal model snapshot")
            (snapshot-round opts)
            (gen/sleep 1)
            (gen/log "Collecting stable terminal model snapshot")
            (snapshot-round opts)))))

(defn workload [opts]
  (let [revision (atom 3000)
        active (->> (repeatedly #(mutation revision (:key-count opts) (:owner-count opts)))
                    (gen/stagger 0.005)
                    (gen/nemesis (fault-cycle (:fault-interval opts)))
                    (gen/time-limit (:time-limit opts)))]
    (apply gen/phases (concat (prelude) [active] (terminal-phases opts)))))

(defn group-test [opts]
  (let [db (group-db/db)
        permanent? (= "permanent" (:scenario opts))
        terminal-nodes (if permanent? (vec (rest (:nodes opts))) (:nodes opts))
        retired-nodes (if permanent? [(first (:nodes opts))] [])
        opts (assoc opts
                    :clusters clusters
                    :terminal-nodes terminal-nodes
                    :retired-nodes retired-nodes)]
    (merge tests/noop-test
           opts
           {:name (str "group lifecycle convergence (" (:transport opts) "/"
                       (:scenario opts) ")")
            :os os/noop
            :db db
            :client (group-client/client)
            :nemesis (group-nemesis/nemesis db)
            :pure-generators true
            :generator (workload opts)
            :checker (model/checker)})))

(def cli-options
  [[nil "--key-count NUMBER" "Number of keys in each cluster and data type"
    :default 8
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]
   [nil "--fault-interval SECONDS" "Seconds between fault transitions"
    :default 2
    :parse-fn #(Double/parseDouble %)
    :validate [pos? "Must be positive"]]
   [nil "--owner-count NUMBER" "Logical owner slots per node"
    :default 32
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]
   [nil "--recovery-time SECONDS" "Fault-free convergence time before checking"
    :default 8
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]
   [nil "--transport PROFILE" "Replica transport: distribution, tcp, or chaos"
    :default "distribution"
    :validate [#{"distribution" "tcp" "chaos"} "Unsupported transport"]]
   [nil "--scenario SCENARIO" "Lifecycle scenario: mixed or permanent"
    :default "mixed"
    :validate [#{"mixed" "permanent"} "Unsupported scenario"]]
   [nil "--corruption MODE" "Checker qualification: none, unexpected-death, internal-index"
    :default "none"
    :validate [#{"none" "unexpected-death" "internal-index"} "Unsupported corruption"]]
   [nil "--max-operation-latency-ms MILLIS" "Maximum acknowledged Group call latency"
    :default 2000
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]])

(defn -main [& args]
  (cli/run!
    (cli/single-test-cmd {:test-fn group-test, :opt-spec cli-options})
    args))
