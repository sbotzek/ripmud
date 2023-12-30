(ns ripmud.server
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

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


(def *systems (atom []))

(defn run-system
  [system pulse]
  (let [{:keys [f f-arg name pulses uses-components require-components updates-components]} system]
    (when (zero? (mod pulse pulses))
      (try
        (let [start-time (System/currentTimeMillis)
              *select-ms (atom nil)
              *run-f-ms (atom nil)
              *update-ms (atom nil)]
          (case f-arg
            ;; system function takes argument of the form: {entity_1 {component}, entity_2 {component}, ....}
            :entities->component
            (let [select-start-time (System/currentTimeMillis)
                  entities->components (get @*components (first uses-components))
                  _ (reset! *select-ms (- (System/currentTimeMillis) select-start-time))
                  run-f-start-time (System/currentTimeMillis)
                  components' (f entities->components)
                  _ (reset! *run-f-ms (- (System/currentTimeMillis) run-f-start-time))]
              (when (not= (count updates-components) 0)
                (let [update-start-time (System/currentTimeMillis)]
                  (dosync
                   (alter *components assoc (first updates-components) components'))
                  (reset! *update-ms (- (System/currentTimeMillis) update-start-time)))))

            ;; system function takes argument of the form:
            ;; {component_type_1 {entity_1 {component}, entity_2 {component}, ...},
            ;;  component_type_2 {entity_1 {component}, entity_2 {component}, ...},
            ;;  ...}
            :types->entities->component
            (let [select-start-time (System/currentTimeMillis)
                  types->entities->component (select-keys @*components uses-components)
                  _ (reset! *select-ms (- (System/currentTimeMillis) select-start-time))
                  run-f-start-time (System/currentTimeMillis)
                  components' (f types->entities->component)
                  _ (reset! *run-f-ms (- (System/currentTimeMillis) run-f-start-time))]
              (let [update-start-time (System/currentTimeMillis)]
                (dosync
                 (doseq [comp updates-components]
                   (alter *components update comp merge (get components' comp))))
                (reset! *update-ms (- (System/currentTimeMillis) update-start-time)))))
          (let [total-ms (- (System/currentTimeMillis) start-time)]
            (println "System" name "total ms:" total-ms "select ms:" @*select-ms "run-f ms:" @*run-f-ms "update ms:" @*update-ms)))
        (catch Exception e
          (throw (ex-info (str "Error running system " name) {:system system :pulse pulse :exception e})))))))

(defn validate-system
  [{:keys [f-arg name uses-components] :as system}]
  (case f-arg
    :entities->component
    (do
      ;; only works for systems that require a single component
      (when (not= 1 (count uses-components))
        (throw (ex-info (str "f-arg " f-arg " requires exactly one uses components") {:system system})))
      (when (> (count uses-components) 1)
        (throw (ex-info (str "f-arg " f-arg " requires one or more updates components") {:system system}))))

    :types->entities->component
    nil

    (throw (ex-info (str "unknown f-arg: " f-arg) {:system system}))))


(defn run-game-server
  [config]
  (dorun (map validate-system @*systems))
  (loop [game-state {}
         pulse 1]
    #_(println "entities" @*entities)
    #_(println "entity-components" @*entity-components)
    #_(println "components" @*components)
    (let [start-time (System/currentTimeMillis)]
      (dorun (map #(run-system % pulse) @*systems))
      (let [elapsed-time (- (System/currentTimeMillis) start-time)]
        (println "Sleeping For" (- millis-per-pulse elapsed-time) "ms")
        (when (< elapsed-time millis-per-pulse)
          (Thread/sleep (- millis-per-pulse elapsed-time)))
        (recur game-state (inc pulse))))))

(def telnet-inputs (java.util.concurrent.ConcurrentLinkedQueue.))

(defn slurp-telnet-inputs
  "Reads from the global telnet-inputs queue and puts those lines into the entity's telnet-input component."
  [components]
  (loop [components' {}]
    (if-let [{:keys [entity-id line]} (.poll telnet-inputs)]
      (let [telnet-input (get-in components' [:telnet-input entity-id])]
        (recur (assoc-in components' [:telnet-input entity-id] (update telnet-input :input conj line))))
      components')))

(defn process-telnet-inputs
  "Takes input from the telnet-input component and processes the command, writing any output to the telnet-output component."
  [components]
  (let [*telnet-input-components (atom {})
        *telnet-output-components (atom {})]
    (doseq [[entity telnet-input] (:telnet-input components)]
      (let [telnet-output (get-in components [:telnet-output entity])]
        (when-let [[line & rest] (:input telnet-input)]
          (when (and line (not= "" line))
            (let [[cmd & args] (str/split line #"\s+")
                  cmd (str/lower-case cmd)
                  output (cond
                           (= "jimmie" cmd)
                           "JIMMIEEEE! JIMMIE JIMMIE JIMMIESON!\n"

                           true
                           (str "You said: " line "\n"))]
              (swap! *telnet-output-components assoc entity (update telnet-output :output conj output))))
          (swap! *telnet-input-components assoc entity (assoc telnet-input :input rest)))))
    {:telnet-input @*telnet-input-components
     :telnet-output @*telnet-output-components}))

(defn write-telnet-outputs
  "Takes output from the telnet-output component and writes it to the socket."
  [components]
  (let [*telnet-output-components (atom components)]
    (doseq [[entity {:keys [output out] :as telnet-output}] components]
      (when (seq output)
        (swap! *telnet-output-components assoc entity (assoc telnet-output :output []))
        (doseq [line output]
          (.write out line))
        (.flush out)))
    @*telnet-output-components))

(defn update-lifetimes
  [components]
  (update-vals components (fn [lifetime-tracker] (update lifetime-tracker :pulses inc))))

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
  (reset! *systems
          [
           {:f update-lifetimes
            :f-arg :entities->component
            :name "update-lifetimes"
            :pulses 1
            :uses-components [:lifetime-tracker]
            :require-components [:lifetime-tracker]
            :updates-components [:lifetime-tracker]}
           {:f slurp-telnet-inputs
            :f-arg :types->entities->component
            :name "slurp-telnet-inputs"
            :pulses 1
            :uses-components [:telnet-input]
            :require-components [:telnet-input]
            :updates-components [:telnet-input]}
           {:f process-telnet-inputs
            :f-arg :types->entities->component
            :name "process-telnet-inputs"
            :pulses 1
            :uses-components [:telnet-input :telnet-output]
            :require-components [:telnet-input :telnet-output]
            :updates-components [:telnet-input :telnet-output]}
           {:f write-telnet-outputs
            :f-arg :entities->component
            :name "write-telnet-outputs"
            :pulses 1
            :uses-components [:telnet-output]
            :require-components [:telnet-output]
            :updates-components [:telnet-output]}
           ])
  ;; uncomment for performance testing
  #_(dotimes [n 100000]
    (let [entity (new-entity!)]
      (add-component! entity :lifetime-tracker {:pulses 0})
      (add-component! entity :telnet-input {:input []})
      (add-component! entity :telnet-output {:out nil :output []}))
    )
  (let [config (edn/read-string (slurp (io/resource "server-config.edn")))
        telnet-thread (Thread/startVirtualThread
                       (fn telnet-handler[]
                         (run-telnet-server config)))
        game-thread (Thread/startVirtualThread
                     (fn game-handler[]
                       (run-game-server config)))]
    (.join game-thread)))
