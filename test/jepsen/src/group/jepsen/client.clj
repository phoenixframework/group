(ns group.jepsen.client
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [group.jepsen.docker :as docker]
            [jepsen.client :as client])
  (:import (java.io DataInputStream DataOutputStream)
           (java.net InetSocketAddress Socket)
           (java.nio.charset StandardCharsets)))

(defn request!
  ([node fields] (request! node fields 3000))
  ([node fields timeout-ms]
   (with-open [socket (Socket.)]
     (let [^String host "127.0.0.1"]
       (.connect socket (InetSocketAddress. host (int (docker/port node))) 1000))
     (.setSoTimeout socket timeout-ms)
     (let [payload (.getBytes (str/join "\t" fields) StandardCharsets/UTF_8)
           out (DataOutputStream. (.getOutputStream socket))
           in (DataInputStream. (.getInputStream socket))]
       (.writeInt out (alength payload))
       (.write out payload)
       (.flush out)
       (let [length (.readInt in)
             response (byte-array length)]
         (.readFully in response)
         (edn/read-string (String. response StandardCharsets/UTF_8)))))))

(defn wait-ready!
  ([node expected] (wait-ready! node expected 30000))
  ([node expected timeout-ms]
   (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
     (loop []
       (let [response (try
                        (request! node ["ready" (str expected)] 1000)
                        (catch Exception _ nil))]
         (cond
           (= :ok (:status response)) true
           (< (System/currentTimeMillis) deadline)
           (do (Thread/sleep 100) (recur))
           :else
           (throw (ex-info "Group node did not become ready"
                           {:node node, :last-response response}))))))))

(defn wait-listening!
  ([node] (wait-listening! node 15000))
  ([node timeout-ms]
   (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
     (loop []
       (if (try
             (= :ok (:status (request! node ["ping"] 1000)))
             (catch Exception _ false))
         true
         (if (< (System/currentTimeMillis) deadline)
           (do (Thread/sleep 100) (recur))
           (throw (ex-info "Group node did not start listening" {:node node}))))))))

(defn cluster-field [cluster]
  (if (nil? cluster) "root" cluster))

(defn command [op]
  (let [logical-owner (str (or (get-in op [:value :owner]) (:process op)))
        cluster (cluster-field (get-in op [:value :cluster]))]
    (case (:f op)
      :register
      ["mutate" "register" logical-owner cluster
       (str (get-in op [:value :key]))
       (str (get-in op [:value :revision]))]

      :unregister
      ["mutate" "unregister" logical-owner cluster
       (str (get-in op [:value :key])) "0"]

      :join
      ["mutate" "join" logical-owner cluster
       (str (get-in op [:value :key]))
       (str (get-in op [:value :revision]))]

      :leave
      ["mutate" "leave" logical-owner cluster
       (str (get-in op [:value :key])) "0"]

      :kill
      ["kill" logical-owner]

      :cluster-connect
      ["cluster" "connect" (get-in op [:value :cluster])]

      :cluster-disconnect
      ["cluster" "disconnect" (get-in op [:value :cluster])]

      :connect-all
      ["cluster" "connect-all"]

      :corrupt
      ["corrupt" (name (get-in op [:value :mode]))]

      :snapshot
      ["snapshot"
       (str (get-in op [:value :key-count]))
       (str/join "," (get-in op [:value :clusters]))
       (->> (get-in op [:value :retired-nodes])
            (map #(str "group@" (name %)))
            (str/join ","))])))

(defrecord GroupClient [node]
  client/Client
  (open! [this _test node] (assoc this :node (name node)))
  (setup! [this _test] this)

  (invoke! [_this _test op]
    (let [target (name (or (get-in op [:value :target]) node))]
      (try
        (let [response (request! target (command op) 10000)
              value (if (= :snapshot (:f op))
                      (:snapshot response)
                      {:request (:value op), :response response, :node target})]
          (case (:status response)
            :ok (assoc op :type :ok, :value value)
            :fail (assoc op :type :fail, :value value, :error (:error response))
            (assoc op :type :info, :value value, :error (:error response))))
        (catch Exception exception
          (assoc op :type :info
                    :error {:class (str (class exception))
                            :message (.getMessage exception)})))))

  (teardown! [this _test] this)
  (close! [_this _test])

  client/Reusable
  (reusable? [_this _test] true))

(defn client [], (GroupClient. nil))
