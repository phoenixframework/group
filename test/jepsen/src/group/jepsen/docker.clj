(ns group.jepsen.docker
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]))

(def containers
  {"n1" "group-jepsen-n1"
   "n2" "group-jepsen-n2"
   "n3" "group-jepsen-n3"})

(def ports
  {"n1" 19081
   "n2" 19082
   "n3" 19083})

(def full-chain "GROUP_JEPSEN_FULL")
(def replica-chain "GROUP_JEPSEN_REPLICA")
(def replica-port 10000)

(defn container [node]
  (or (get containers (name node))
      (throw (ex-info "unknown Jepsen node" {:node node}))))

(defn port [node]
  (or (get ports (name node))
      (throw (ex-info "unknown Jepsen node" {:node node}))))

(defn shell!
  [& args]
  (let [{:keys [exit out err]} (apply shell/sh args)]
    (when-not (zero? exit)
      (throw (ex-info "command failed"
                      {:command args, :exit exit, :out out, :err err})))
    (str/trim out)))

(defn docker!
  [& args]
  (apply shell! "docker" args))

(defn running? [node]
  (= "true"
     (try
       (docker! "inspect" "--format" "{{.State.Running}}" (container node))
       (catch Exception _ "false"))))

(defn start! [node]
  (docker! "start" (container node)))

(defn stop! [node]
  (when (running? node)
    (docker! "stop" "--time" "0" (container node))))

(defn restart! [node]
  (docker! "restart" "--time" "0" (container node)))

(defn ip [node]
  (docker! "inspect"
           "--format"
           "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"
           (container node)))

(defn exec-sh!
  [node script]
  (docker! "exec" (container node) "sh" "-c" script))

(defn reset-oracle! [node]
  (exec-sh! node
            (str "rm -f /tmp/group-jepsen-unexpected-deaths "
                 "/tmp/group-jepsen-persistent-events "
                 "/tmp/group-jepsen-cursor-marker-corruption")))

(defn ensure-firewall-chain! [node chain]
  (exec-sh!
    node
    (str "iptables -N " chain " 2>/dev/null || true; "
         "iptables -C INPUT -j " chain " 2>/dev/null || "
         "iptables -I INPUT 1 -j " chain "; "
         "iptables -C OUTPUT -j " chain " 2>/dev/null || "
         "iptables -I OUTPUT 1 -j " chain)))

(defn flush-chain! [node chain]
  (when (running? node)
    (ensure-firewall-chain! node chain)
    (exec-sh! node (str "iptables -F " chain))))

(defn heal-full! [nodes]
  (doseq [node nodes]
    (flush-chain! node full-chain)))

(defn heal-replica! [nodes]
  (doseq [node nodes]
    (flush-chain! node replica-chain)))

(defn heal! [nodes]
  (heal-full! nodes)
  (heal-replica! nodes))

(defn isolate!
  "Cuts one node off from every other DB node while preserving client traffic."
  [nodes isolated]
  (heal-full! nodes)
  (let [ips (into {} (map (juxt identity ip) nodes))]
    (doseq [node nodes
            peer nodes
            :when (and (not= node peer)
                       (or (= node isolated) (= peer isolated)))]
      (exec-sh!
        node
        (str "iptables -A " full-chain
             " -d " (get ips peer) " -j DROP; "
             "iptables -A " full-chain
             " -s " (get ips peer) " -j DROP")))))

(defn partition-replica!
  "Drops only sideband TCP packets for the supplied directed node pairs."
  [nodes edges]
  (heal-replica! nodes)
  (let [ips (into {} (map (juxt identity ip) nodes))]
    (doseq [[source target] edges]
      (exec-sh!
        source
        (str "iptables -A " replica-chain
             " -p tcp -d " (get ips target)
             " --dport " replica-port " -j DROP")))))
