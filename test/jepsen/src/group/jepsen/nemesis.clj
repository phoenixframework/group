(ns group.jepsen.nemesis
  (:require [group.jepsen.client :as group-client]
            [group.jepsen.docker :as docker]
            [jepsen.db :as db]
            [jepsen.nemesis :as nemesis]))

(defn ordered-pairs [nodes]
  (for [source nodes, target nodes :when (not= source target)] [source target]))

(defn partition-shape [nodes requested]
  (let [nodes (vec nodes)
        shape (or requested (rand-nth [:isolate :all :asymmetric]))]
    (case shape
      :all {:shape :all, :edges (vec (ordered-pairs nodes))}
      :asymmetric
      (let [source (rand-nth nodes)
            target (rand-nth (vec (remove #(= source %) nodes)))]
        {:shape :asymmetric, :edges [[source target]]})
      :isolate
      (let [isolated (rand-nth nodes)]
        {:shape :isolate
         :isolated isolated
         :edges (vec (filter (fn [[source target]]
                               (or (= source isolated) (= target isolated)))
                             (ordered-pairs nodes)))}))))

(defn logical-block! [edges]
  (doseq [[source target] edges]
    (when (docker/running? source)
      (group-client/request! source ["transport" "block" (str "group@" (name target))]))))

(defn logical-heal! [nodes]
  (doseq [node nodes]
    (when (docker/running? node)
      (try
        (group-client/request! node ["transport" "heal"])
        (catch Exception _ nil)))))

(defrecord PartitionNemesis [isolated]
  nemesis/Nemesis
  (setup! [this test]
    (docker/heal-full! (:nodes test))
    this)

  (invoke! [_this test op]
    (case (:f op)
      :start
      (if @isolated
        (assoc op :type :info, :value {:already-isolated @isolated})
        (let [node (rand-nth (vec (:nodes test)))]
          (docker/isolate! (:nodes test) node)
          (reset! isolated node)
          (assoc op :type :info, :value {:isolated node})))

      :stop
      (do
        (docker/heal-full! (:nodes test))
        (let [node @isolated]
          (reset! isolated nil)
          (assoc op :type :info, :value {:healed node})))))

  (teardown! [_this test]
    (docker/heal-full! (:nodes test))))

(defrecord ReplicaNemesis [active]
  nemesis/Nemesis
  (setup! [this test]
    (docker/heal-replica! (:nodes test))
    (logical-heal! (:nodes test))
    this)

  (invoke! [_this test op]
    (case (:f op)
      :start
      (if @active
        (assoc op :type :info, :value {:already-active @active})
        (let [requested (get-in op [:value :shape])
              fault (partition-shape (:nodes test) requested)]
          (if (= "tcp" (:transport test))
            (docker/partition-replica! (:nodes test) (:edges fault))
            (logical-block! (:edges fault)))
          (reset! active fault)
          (assoc op :type :info, :value fault)))

      :stop
      (do
        (docker/heal-replica! (:nodes test))
        (logical-heal! (:nodes test))
        (let [fault @active]
          (reset! active nil)
          (assoc op :type :info, :value {:healed fault})))

      :reset
      (let [nodes (vec (filter docker/running? (:nodes test)))
            source (when (seq nodes) (rand-nth nodes))
            targets (when source (vec (remove #(= source %) nodes)))
            target (when (seq targets) (rand-nth targets))]
        (when (and source target)
          (group-client/request!
            source
            ["transport" "reset" (str "group@" (name target))]))
        (assoc op :type :info, :value {:source source, :target target}))))

  (teardown! [_this test]
    (docker/heal-replica! (:nodes test))
    (logical-heal! (:nodes test))))

(defrecord ProcessNemesis [db killed]
  nemesis/Nemesis
  (setup! [this _test] this)

  (invoke! [_this test op]
    (case (:f op)
      :start
      (if @killed
        (assoc op :type :info, :value {:already-killed @killed})
        (let [node (or (get-in op [:value :node])
                       (rand-nth (vec (:nodes test))))]
          (db/kill! db test node)
          (reset! killed node)
          (assoc op :type :info, :value {:killed node})))

      :stop
      (if-let [node @killed]
        (do
          (db/start! db test node)
          (reset! killed nil)
          (assoc op :type :info, :value {:restarted node}))
        (assoc op :type :info, :value {:restarted nil}))))

  (teardown! [_this test]
    (when-let [node @killed]
      (db/start! db test node)
      (reset! killed nil))))

(defrecord RetirementNemesis [db retired]
  nemesis/Nemesis
  (setup! [this _test] this)

  (invoke! [_this test op]
    (let [node (or (get-in op [:value :node]) (first (:nodes test)))]
      (when (and (nil? @retired) (docker/running? node))
        (db/kill! db test node)
        (reset! retired node))
      (assoc op :type :info, :value {:retired @retired})))

  (teardown! [_this test]
    (when-let [node @retired]
      (db/start! db test node)
      (reset! retired nil))))

(defn nemesis [db]
  (nemesis/compose
    {{:partition-start :start, :partition-stop :stop}
     (PartitionNemesis. (atom nil))

     {:replica-partition-start :start,
      :replica-partition-stop :stop,
      :replica-reset :reset}
     (ReplicaNemesis. (atom nil))

     {:kill-node :start, :restart-node :stop}
     (ProcessNemesis. db (atom nil))

     {:retire-node :retire}
     (RetirementNemesis. db (atom nil))}))
