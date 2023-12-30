(ns ripmud.server
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(def millis-per-pulse 250)

(def *next-entity (ref 0))
(def *entities (ref #{}))

(defn new-entity!
  []
  (dosync
   (let [entity (alter *next-entity inc)]
     (alter *entities conj entity)
     entity)))

;;; Effects are for things that come about from outside a system, like input from a telnet socket.
(def effect-queue (java.util.concurrent.ConcurrentLinkedQueue.))

(def *systems (atom []))

(defn run-system
  [{:keys [pulse effects components] :as game-state} system]
  (try
    (let [start-time (System/currentTimeMillis)
          game-state' (case (:type system)
                        :effect-handler
                        (let [{:keys [f f-arg handle-effects uses-components updates-components]} system]
                          (let [handling-effects (filter #(get handle-effects (:type %)) effects)
                                effects' (filter #(not (get handle-effects (:type %))) effects)]
                            (case f-arg
                              :all-components
                              (let [components' (reduce f components handling-effects)]
                                (assoc game-state
                                       :effects effects'
                                       :components components'))

                              ;; system function takes argument of the form: {entity_1 {component}, entity_2 {component}, ....}
                              :entities->component
                              (let [entities->components (get components (first uses-components))
                                    components' (reduce f entities->components handling-effects)]
                                (assoc game-state
                                       :effects effects'
                                       :components (assoc components (first uses-components) components')))

                              ;; system function takes argument of the form:
                              ;; {component_type_1 {entity_1 {component}, entity_2 {component}, ...},
                              ;;  component_type_2 {entity_1 {component}, entity_2 {component}, ...},
                              ;;  ...}
                              :types->entities->component
                              (let [types->entities->component (select-keys components uses-components)
                                    components' (reduce f types->entities->component handling-effects)]
                                (-> game-state
                                    (assoc :effects effects')
                                    (update :components #(merge % components')))))))

                        :periodic
                        (let [{:keys [f f-arg pulses uses-components updates-components]} system]
                          (if (zero? (mod pulse pulses))
                            (case f-arg
                              ;; system function takes argument of the form: {entity_1 {component}, entity_2 {component}, ....}
                              :entities->component
                              (let [entities->components (get components (first uses-components))
                                    components' (f entities->components)]
                                (assoc game-state
                                       :components (assoc components (first uses-components) components')))

                              ;; system function takes argument of the form:
                              ;; {component_type_1 {entity_1 {component}, entity_2 {component}, ...},
                              ;;  component_type_2 {entity_1 {component}, entity_2 {component}, ...},
                              ;;  ...}
                              :types->entities->component
                              (let [types->entities->component (select-keys components uses-components)
                                    components' (f types->entities->component)]
                                (-> game-state
                                    (update :components #(merge % components')))))
                            game-state)))]
      (println "System" (:name system) "total ms:" (- (System/currentTimeMillis) start-time))
      #_(println "  System" (:name system) "game-state" game-state)
      #_(println "  System" (:name system) "game-state'" game-state)
      game-state')
    (catch Exception e
      (throw (ex-info (str "Error running system " (:name system)) {:system system :exception e :game-state game-state})))))

(defn validate-system
  [{:keys [f-arg type name uses-components] :as system}]
  (case f-arg
    :all-components
    (when (not= :effect-handler type)
      (throw (ex-info (str "f-arg " f-arg " only usable on type :effect-handler") {:system system})))
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

(defn slurp-effects
  [effects]
  (if-let [effect (.poll effect-queue)]
    (recur (conj effects effect))
    effects))

(defn run-game-server
  [config]
  (dorun (map validate-system @*systems))
  (loop [game-state {:pulse 0
                     :components {}
                     :effects []}]
    #_(println "Main Loop Start: game-state" game-state)
    (let [start-time (System/currentTimeMillis)
          game-state' (update game-state :pulse inc)
          game-state' (update game-state' :effects slurp-effects)
          game-state' (reduce run-system game-state' @*systems)]
      (let [elapsed-time (- (System/currentTimeMillis) start-time)]
        (println "Sleeping For" (- millis-per-pulse elapsed-time) "ms")
        (when (< elapsed-time millis-per-pulse)
          (Thread/sleep (- millis-per-pulse elapsed-time)))
        (recur game-state')))))

(defn handle-add-component
  "Handles add component effect by adding the component to the entity"
  [components {:keys [entity component data]}]
  (assoc-in components [component entity] data))

(defn handle-telnet-input
  "Takes telnet input effects and puts them into the correct entity's component."
  [components effect]
  (let [{:keys [entity line]} effect
        telnet-input (get-in components [:telnet-input entity])]
    (assoc-in components [:telnet-input entity] (update telnet-input :input conj line))))

(defn process-telnet-inputs
  "Takes input from the telnet-input component and processes the command, writing any output to the telnet-output component."
  [components]
  (let [*telnet-input-components (atom (:telnet-input components))
        *telnet-output-components (atom (:telnet-output components))]
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
          (swap! *telnet-input-components assoc-in [entity :input] rest))))
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
                                     entity (new-entity!)]
                                 (.offer effect-queue {:type :add-component :entity entity :component :telnet-input :data {:input []}})
                                 (.offer effect-queue {:type :add-component :entity entity :component :telnet-output :data {:out out :output []}})
                                 (while true
                                   (let [line (.readLine in)]
                                     (println (str "Received: " line))
                                     (.offer effect-queue {:type :telnet-input :entity entity :line line}))))))])))))

(defn -main
  []
  (reset! *systems
          [
           {:f handle-add-component
            :f-arg :all-components
            :type :effect-handler
            :name "handle-add-component"
            :handle-effects #{:add-component}}
           {:f update-lifetimes
            :f-arg :entities->component
            :type :periodic
            :name "update-lifetimes"
            :pulses 1
            :uses-components [:lifetime-tracker]
            :updates-components [:lifetime-tracker]}
           {:f handle-telnet-input
            :f-arg :types->entities->component
            :type :effect-handler
            :name "handle-telnet-input"
            :handle-effects #{:telnet-input}
            :uses-components [:telnet-input]
            :updates-components [:telnet-input]}
           {:f process-telnet-inputs
            :f-arg :types->entities->component
            :type :periodic
            :name "process-telnet-inputs"
            :pulses 1
            :uses-components [:telnet-input :telnet-output]
            :updates-components [:telnet-input :telnet-output]}
           {:f write-telnet-outputs
            :f-arg :entities->component
            :type :periodic
            :name "write-telnet-outputs"
            :pulses 1
            :uses-components [:telnet-output]
            :updates-components [:telnet-output]}
           ])
  ;; uncomment for performance testing
  #_(dotimes [n 100000]
    (let [entity (new-entity!)]
      (.offer effect-queue {:type :add-component :entity entity :component :lifetime-tracker :data {:pulses 0}})
      (.offer effect-queue {:type :add-component :entity entity :component :telnet-input :data {:input []}})
      (.offer effect-queue {:type :add-component :entity entity :component :telnet-output :data {:out nil :output []}})))
  (let [config (edn/read-string (slurp (io/resource "server-config.edn")))
        telnet-thread (Thread/startVirtualThread
                       (fn telnet-handler[]
                         (run-telnet-server config)))
        game-thread (Thread/startVirtualThread
                     (fn game-handler[]
                       (run-game-server config)))]
    (.join game-thread)))
