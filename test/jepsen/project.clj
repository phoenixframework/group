(defproject group-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen lifecycle and convergence tests for Group"
  :url "https://github.com/phoenixframework/group"
  :license {:name "MIT"}
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [jepsen "0.3.13"]]
  :main group.jepsen.core
  :jvm-opts ["-Xmx4g" "-Djava.awt.headless=true" "-server"])
