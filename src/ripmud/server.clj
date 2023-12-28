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
  (let [{:keys [f name pulses uses-components require-components updates-components]} system]
    (when (zero? (mod pulse pulses))
      (let [start-time (System/currentTimeMillis)
            entities (map first (filter (fn [[k v]] (every? v require-components)) @*entity-components))
            components-examining (select-keys @*components uses-components)
            components-and-entities-examining (into {} (map (fn [[k v]] [k (select-keys v entities)]) components-examining))
            components' (f components-and-entities-examining)
            elapsed-time (- (System/currentTimeMillis) start-time)]
        (println "System" name "took" elapsed-time "ms, started with" components-and-entities-examining "returned" components')
        (dosync
         (alter *components merge (select-keys components' updates-components)))))))

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
  (if-let [{:keys [entity-id line]} (.poll telnet-inputs)]
    (recur (update-in components [:telnet-input entity-id :input] conj line))
    components))

(defn process-telnet-inputs
  [components]
  (let [*telnet-input-components (atom (:telnet-input components))
        *telnet-output-components (atom (:telnet-output components))]
    (doseq [[entity telnet-input] @*telnet-input-components]
      (let [telnet-output (get-in components [:telnet-output entity])]
        (when-let [[line & rest] (:input telnet-input)]
          (when (and line (not= "" line))
            (swap! *telnet-output-components update-in [entity :output] conj (str "You said: " line "\n")))
          (swap! *telnet-input-components assoc entity {:input rest}))))
    {:telnet-input @*telnet-input-components
     :telnet-output @*telnet-output-components}))

(defn write-telnet-outputs
  [components]
  (let [*telnet-output-components (atom (:telnet-output components))]
    (doseq [[entity {:keys [output out] :as telnet-output}] @*telnet-output-components]
      (when (seq output)
        (swap! *telnet-output-components assoc-in [entity :output] [])
        (doseq [line output]
          (.write out line))
        (.flush out)))
    {:telnet-output @*telnet-output-components}))

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
            :name "slurp-telnet-inputs"
            :pulses 1
            :uses-components [:telnet-input]
            :require-components [:telnet-input]
            :updates-components [:telnet-input]}
           {:f process-telnet-inputs
            :name "process-telnet-inputs"
            :pulses 1
            :uses-components [:telnet-input :telnet-output]
            :require-components [:telnet-input :telnet-output]
            :updates-components [:telnet-input :telnet-output]}
           {:f write-telnet-outputs
            :name "write-telnet-outputs"
            :pulses 1
            :uses-components [:telnet-output]
            :require-components [:telnet-output]
            :updates-components [:telnet-output]}])
  (let [config (edn/read-string (slurp (io/resource "server-config.edn")))
        telnet-thread (Thread/startVirtualThread
                       (fn telnet-handler[]
                         (run-telnet-server config)))
        game-thread (Thread/startVirtualThread
                     (fn game-handler[]
                       (run-game-server config)))]
    (.join game-thread)))
