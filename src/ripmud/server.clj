(ns ripmud.server
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]))

(defn start
  [{:keys [port] :as config}]
  (let [server-socket (java.net.ServerSocket. port)]
    (println (str "Server started on port " port))
    (while true
      (let [client-socket (.accept server-socket)]
        (println (str "Client connected from " (.getInetAddress client-socket)))
        (let [client-thread (Thread/startVirtualThread
                             (fn client-handler[]
                               (let [in (io/reader client-socket)
                                     out (io/writer client-socket)]
                                 (while true
                                   (let [line (.readLine in)]
                                     (println (str "Received: " line))
                                     (.write out (str "You said: " line "\n"))
                                     (.flush out))))))])))))

(defn -main
  []
  (let [config (edn/read-string (slurp (io/resource "server-config.edn")))]
    (start config)))
