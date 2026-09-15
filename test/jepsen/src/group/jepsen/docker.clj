(ns group.jepsen.docker
  (:require [clojure.edn :as edn]
            [clojure.string :as str])
  (:import (java.io File)
           (java.util.concurrent TimeUnit)))

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

(def ^:dynamic *command-timeout-ms* 30000)

(defn container [node]
  (or (get containers (name node))
      (throw (ex-info "unknown Jepsen node" {:node node}))))

(defn port [node]
  (or (get ports (name node))
      (throw (ex-info "unknown Jepsen node" {:node node}))))

(defn shell!
  [& args]
  (let [output (File/createTempFile "group-jepsen-command-" ".log")
        errors (File/createTempFile "group-jepsen-command-" ".err")]
    (try
      (let [process (-> (ProcessBuilder. ^java.util.List (vec args))
                        (.redirectError errors)
                        (.redirectOutput output)
                        .start)]
        (try
          (when-not (.waitFor process *command-timeout-ms* TimeUnit/MILLISECONDS)
            (throw (ex-info "command timed out"
                            {:command args :timeout-ms *command-timeout-ms*})))
          (let [out (slurp output)
                exit (.exitValue process)]
            (when-not (zero? exit)
              (throw (ex-info "command failed"
                              {:command args :exit exit :out out :err (slurp errors)})))
            (str/trim out))
          (finally
            (when (.isAlive process)
              (.destroyForcibly process)
              (.waitFor process 1000 TimeUnit/MILLISECONDS)))))
      (finally
        (.delete output)
        (.delete errors)))))

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
            (str "rm -f /tmp/group-jepsen-persistent-events "
                 "/tmp/group-jepsen-cursor-marker-corruption && "
                 ": > /tmp/group-jepsen-unexpected-deaths")))

(defn parse-unexpected-deaths [contents]
  (mapv (fn [line]
          (let [[token reason :as fields] (str/split line #"\t" 2)]
            (when (or (not= 2 (count fields)) (str/blank? token) (str/blank? reason))
              (throw (ex-info "malformed lifecycle evidence" {:line line})))
            {:token token :reason reason}))
        (remove str/blank? (str/split-lines contents))))

(defn decode-conflict-evidence! [file]
  (let [output (shell! "sh" "-c"
                       (str "cd ../.. && exec env ERL_FLAGS='+S 2:2' mix run --no-start "
                            "test/jepsen/decode_conflict_evidence.exs \"$1\"")
                       "_" (.getAbsolutePath ^File file))
        prefix "CONFLICT-EVIDENCE "
        line (first (filter #(str/starts-with? % prefix) (str/split-lines output)))]
    (when-not line
      (throw (ex-info "missing decoded conflict evidence" {})))
    (edn/read-string (subs line (count prefix)))))

(defn collect-evidence! [node path bound decode]
  (let [file (File/createTempFile "group-jepsen-retired-" ".log")]
    (try
      (binding [*command-timeout-ms* 10000]
        (docker! "cp"
                 (str (container node) ":" path)
                 (.getAbsolutePath file)))
      (when (> (.length file) bound)
        (throw (ex-info "lifecycle evidence exceeds collection bound" {:node node})))
      (let [contents (slurp file)]
        (when (and (seq contents) (not (str/ends-with? contents "\n")))
          (throw (ex-info "truncated lifecycle evidence" {:node node})))
        (binding [*command-timeout-ms* 10000]
          (decode file)))
      (finally (.delete file)))))

(defn retired-evidence!
  "Reads the stopped container's durable oracle, without depending on its VM or socket.
  Missing, truncated, unreadable, or oversized evidence is a qualification failure."
  [node]
  {:node (name node)
   :unexpected-deaths
   (collect-evidence! node "/tmp/group-jepsen-unexpected-deaths" (* 8 1024 1024)
                      #(parse-unexpected-deaths (slurp %)))
   :conflict-evidence
   (collect-evidence! node "/tmp/group-jepsen-conflict-evidence" (* 64 1024 1024)
                      decode-conflict-evidence!)})

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
