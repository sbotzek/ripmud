(ns ripmud.server
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]))

(def millis-per-pulse 250)

(def *next-entity-id (ref 0))
(def *entities (ref #{}))
(def *entity-components (ref {}))
(def *components (ref {}))

(defn new-entity!
  []
  (dosync
   (let [entity-id (alter *next-entity-id inc)]
     (alter *entities conj entity-id)
     (alter *entity-components assoc entity-id #{})
     entity-id)))

(defn remove-entity!
  [entity]
  (dosync
   (alter *entities disj entity)
   (doseq [component-key (get @*entity-components entity)]
     (alter *components update component-key dissoc entity))
   (alter *entity-components dissoc entity)))

(defn add-component!
  [entity k component]
  (dosync
   (alter *entity-components update entity conj k)
   (alter *components assoc-in [k entity] component)))

(defn remove-component!
  [entity k]
  (dosync
   (alter *entity-components update entity disj k)
   (alter *components update k dissoc entity)))


(def systems (atom []))

(defn run-system
  [system pulse]
  (let [{:keys [f pulses components updates-components]} system]
    (when (zero? (mod pulse pulses))
      (let [entities (map first (filter (fn [[k v]] (every? v components)) @*entity-components))
            components-examining (select-keys @*components components)
            components-and-entities-examining (into {} (map (fn [[k v]] [k (select-keys v entities)]) components-examining))
            components' (f components-and-entities-examining)]
        (dosync
         (alter *components merge components'))))))

(defn run-game-server
  [config]
  (loop [game-state {}
         pulse 1]
    #_(println "entities" @*entities)
    #_(println "entity-components" @*entity-components)
    #_(println "components" @*components)
    (let [start-time (System/currentTimeMillis)]
      (dorun (map #(run-system % pulse) @systems))
      (let [elapsed-time (- (System/currentTimeMillis) start-time)]
        (when (< elapsed-time millis-per-pulse)
          (println "Sleeping for" (- millis-per-pulse elapsed-time) "ms")
          (Thread/sleep (- millis-per-pulse elapsed-time)))
        (recur game-state (inc pulse))))))

(def telnet-inputs (java.util.concurrent.ConcurrentLinkedQueue.))

(defn slurp-telnet-inputs
  [components]
  #_(println "slurp-telnet-inputs - start -" components)
  (if-let [{:keys [entity-id line]} (.poll telnet-inputs)]
    (recur (update-in components [:telnet-input entity-id :input] conj line))
    (do
      #_(println "slurp-telnet-inputs - end -" components)
      components)))

(defn process-telnet-inputs
  [components]
  #_(println "process-telnet-inputs - start -" components)
  (let [*telnet-input-components (atom (:telnet-input components))]
    (doseq [[entity telnet-input] @*telnet-input-components]
      (let [telnet-output (get-in components [:telnet-output entity])]
        (when-let [[line & rest] (:input telnet-input)]
          (when (and line (not= "" line))
            (.write (:out telnet-output) (str "You said: " line "\n"))
            (.flush (:out telnet-output)))
          (swap! *telnet-input-components assoc entity {:input rest}))))
    #_(println "process-telnet-inputs - end -" @*telnet-input-components)
    (assoc components :telnet-input @*telnet-input-components)))

(defn run-telnet-server
  [{:keys [port] :as config}]
  (let [server-socket (java.net.ServerSocket. port)]
    (println (str "Server started on port " port))
    (while true
      (let [client-socket (.accept server-socket)]
        (println (str "Client connected from " (.getInetAddress client-socket)))
        (let [client-thread (Thread/startVirtualThread
                             (fn client-handler[]
                               (let [in (io/reader client-socket)
                                     out (io/writer client-socket)
                                     entity-id (new-entity!)]
                                 (add-component! entity-id :telnet-input {:input []})
                                 (add-component! entity-id :telnet-output {:out out :output []})
                                 (while true
                                   (let [line (.readLine in)]
                                     (println (str "Received: " line))
                                     (.offer telnet-inputs {:entity-id entity-id :line line}))))))])))))

(defn -main
  []
  (reset! systems
          [{:f slurp-telnet-inputs
            :pulses 1
            :components [:telnet-input]
            :updates-components [:telnet-input]}
           {:f process-telnet-inputs
            :pulses 1
            :components [:telnet-input :telnet-output]
            :updates-components [:telnet-input]}])
  (let [config (edn/read-string (slurp (io/resource "server-config.edn")))
        telnet-thread (Thread/startVirtualThread
                       (fn telnet-handler[]
                         (run-telnet-server config)))
        game-thread (Thread/startVirtualThread
                     (fn game-handler[]
                       (run-game-server config)))]
    (.join game-thread)))
