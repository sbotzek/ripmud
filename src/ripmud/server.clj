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


(defn system-components-arg
  "Returns the components args a system needs."
  [{:keys [f-arg uses-components] :as system} components]
  (case f-arg
    ;; system function takes argument of the form: {entity_1 {component}, entity_2 {component}, ....}
    :entities->component
    (case uses-components
      :all (throw (ex-info (str "f-arg " f-arg " cannot use :all uses-component") {:system system}))
      (get components (first uses-components)))

    ;; system function takes argument of the form:
    ;; {component_type_1 {entity_1 {component}, entity_2 {component}, ...},
    ;;  component_type_2 {entity_1 {component}, entity_2 {component}, ...},
    ;;  ...}
    :types->entities->component
    (case uses-components
      :all components
      (select-keys components uses-components))

    (throw (ex-info (str "unknown f-arg: " f-arg) {:system system}))))

(defn system-update-components
  "Updates the components from the results of calling a system ."
  [{:keys [f-arg updates-components] :as system} components system-result]
  (case f-arg
    :entities->component
    (case updates-components
      :all (throw (ex-info (str "f-arg " f-arg " cannot use :all updates-component") {:system system}))
      (assoc components (first updates-components) system-result))

    :types->entities->component
    (case updates-components
      :all system-result
      (merge components (select-keys system-result updates-components)))

    (throw (ex-info (str "unknown f-arg: " f-arg) {:system system}))))

(defn run-system
  "Runs a system."
  [{:keys [pulse effects components] :as game-state} system]
  (try
    (let [start-time (System/currentTimeMillis)
          game-state' (case (:type system)
                        :effect-handler
                        (let [{:keys [f handle-effects]} system]
                          (let [handling-effects (filter #(get handle-effects (:type %)) effects)
                                effects' (filter #(not (get handle-effects (:type %))) effects)
                                components-arg (system-components-arg system components)
                                system-result (reduce f components-arg handling-effects)
                                components' (system-update-components system components system-result)]
                            (assoc game-state
                                   :effects effects'
                                   :components components')))

                        :periodic
                        (let [{:keys [f pulses]} system]
                          (if (zero? (mod pulse pulses))
                            (let [components-arg (system-components-arg system components)
                                  system-result (f components-arg)
                                  components' (system-update-components system components system-result)]
                              (assoc game-state
                                     :components components'))
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
    #_(println "game-state" game-state)
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

(def player-component-type :telnet-state)
(defn player?
  "Returns true if the entity is a player."
  [entity components]
  (not (nil? (get-in components [player-component-type entity]))))

(defprotocol TelnetState
  (telnet-state-prompt [state])
  (telnet-state-entered [state telnet-input telnet-output])
  (telnet-state-input [state telnet-input telnet-output command-queue])
  (telnet-state-left [state telnet-input telnet-output]))

(def command-table
  [{:name "say"
    :restrictions []
    :args :arg-str
    :f (fn [components entity arg-str]
         (update-in components [:perceptor entity :perceptions] concat [{:act :say :actor entity :message arg-str}]))}
   {:name "jimmie"
    :restrictions [:player]
    :args :none
    :f (fn [components entity arg-str]
         (update-in components [:telnet-output entity :output] concat ["JIMMMIEEEE! JIMMIE JIMMIE JIMMIESON!\r\n"]))}

   {:name "components"
    :restrictions [:player]
    :args :arg-list
    :f (fn [components entity arg-str]
         (let [target (if (seq arg-str)
                        (parse-long (first arg-str))
                        entity)
               output (reduce (fn [components-str [component-key components]]
                   (if-let [component (get components target)]
                     (str components-str component-key ": " (get components target) "\r\n")
                     components-str))
                 ""
                 components)]
            (update-in components [:telnet-output entity :output] concat [output])))}
   {:name "quit"
    :restrictions [:player]
    :args :str-cmd
    :f (fn [components entity str-cmd]
         (update-in components [:telnet-output entity :output] concat ["Quit not implemented, you're stuck here forever!\r\n"]))}])

(defn can-use-cmd?
  [cmd entity components]
  (every? (fn [restriction]
            (case restriction
              :player (player? entity components)
              (throw (ex-info (str "Unknown restriction: " restriction) {:restriction restriction}))))
          (:restrictions cmd)))

(defrecord TelnetStatePlaying [name]
  TelnetState
  (telnet-state-prompt [state] "> ")
  (telnet-state-entered [state telnet-input telnet-output]
    [state telnet-input (update telnet-output :output concat [(str "Welcome " name "!\r\n")])])
  (telnet-state-input [state {:keys [input] :as telnet-input} telnet-output command-queue]
    (loop [[line & input'] input
           command-queue' command-queue]
      (if line
        (let [[str-cmd arg-str] (str/split line #"\s+" 2)
              str-cmd (str/lower-case str-cmd)]
          (if-let [cmd (first (filter #(str/starts-with? (:name %) str-cmd) command-table))]
            (let [args (case (:args cmd)
                         :none nil
                         :arg-str arg-str
                         :arg-list (if (nil? arg-str) '() (str/split arg-str #"\s+"))
                         :str-cmd str-cmd)]
              (recur input' (update command-queue' :commands concat [{:command cmd :args args}])))
            (recur input' (update command-queue' :commands concat [{:command nil :str-cmd str-cmd}]))))
        [state nil telnet-output command-queue'])))
  (telnet-state-left [state telnet-input telnet-output]))

(deftype TelnetStateConnected []
    TelnetState
  (telnet-state-prompt [state]
    "What is your name? ")
  (telnet-state-entered [state telnet-input telnet-output]
    [state telnet-input (update telnet-output :output concat ["Welcome to RIPMUD!\r\n"])])
  (telnet-state-input [state {:keys [input] :as telnet-input} telnet-output command-queue]
    (if-let [line (first input)]
      [(TelnetStatePlaying. line) (update telnet-input :input rest) telnet-output]
      [state telnet-input telnet-output]))
  (telnet-state-left [state telnet-input telnet-output]))


(defn handle-telnet-connection
  [components {:keys [entity out]}]
  (let [telnet-state (TelnetStateConnected.)
        telnet-input {:input []}
        telnet-output {:out out
                       :output []}
        [telnet-state telnet-input telnet-output] (telnet-state-entered telnet-state telnet-input telnet-output)
        components' (-> components
                        (assoc-in [:telnet-state entity] telnet-state)
                        (assoc-in [:telnet-input entity] telnet-input)
                        (assoc-in [:telnet-output entity] telnet-output))]
    components'))

(defn process-telnet-inputs
  "Takes input from the telnet-input component and processes the command, writing any output to the telnet-output component."
  [components]
  (let [*components (atom components)]
    (doseq [[entity telnet-input] (:telnet-input components)]
      (when (seq (:input telnet-input))
        (let [telnet-output (get-in components [:telnet-output entity])
              telnet-state (get-in components [:telnet-state entity])
              command-queue (get-in components [:command-queue entity])]
          (let [[telnet-state' telnet-input' telnet-output' command-queue'] (telnet-state-input telnet-state telnet-input telnet-output command-queue)
                [telnet-state' telnet-input' telnet-output'] (if (= (type telnet-state) (type telnet-state'))
                                                               [telnet-state' telnet-input' telnet-output']
                                                               ;; call entered
                                                               (telnet-state-entered telnet-state' telnet-input' telnet-output'))]
            (swap! *components assoc-in [:telnet-state entity] telnet-state')
            (swap! *components assoc-in [:telnet-input entity] telnet-input')
            (swap! *components assoc-in [:telnet-output entity] telnet-output')
            (swap! *components assoc-in [:command-queue entity] command-queue')))))
    @*components))

(defn process-command-queue
  [components]
  (let [*components (atom components)]
    (doseq [[entity command-queue] (:command-queue components)]
      (when (seq (:commands command-queue))
        (let [[{:keys [command args str-cmd]} & commands'] (:commands command-queue)]
          (if commands'
            (swap! *components assoc-in [:command-queue entity] commands')
            (swap! *components update :command-queue dissoc entity))
          (if (and command (can-use-cmd? command entity @*components))
            (let [command-f (:f command)]
              (swap! *components command-f entity args))
            (swap! *components update-in [:telnet-output entity :output] concat [(str "Unknown command: " str-cmd "\r\n")])))))
    @*components))

(defn process-player-perceptions
  "Takes perceptions from the perceptor component and writes them to the telnet-output component."
  [components]
  (let [*components (atom components)]
    (doseq [entity (keys (player-component-type components))]
      (let [perceptions (get-in components [:perceptor entity :perceptions])]
        (when (seq perceptions)
          (swap! *components assoc-in [:perceptor entity :perceptions] [])
          (let [telnet-output (get-in components [:telnet-output entity])]
            (doseq [{:keys [act actor] :as action} perceptions]
              (case act
                :say
                (if (= actor entity)
                  (swap! *components update-in [:telnet-output entity :output] concat [(str "You say \"" (:message action) "\"\r\n")])
                  (swap! *components update-in [:telnet-output entity :output] concat [(str actor " says \"" (:message action) "\"\r\n")]))
                (throw (ex-info (str "Unknown act: " act) {:act act}))))))))
    @*components))

(defn write-telnet-outputs
  "Takes output from the telnet-output component and writes it to the socket."
  [components]
  (let [*components (atom components)]
    (doseq [[entity {:keys [output out] :as telnet-output}] (:telnet-output components)]
      (when (seq output)
        (swap! *components assoc-in [:telnet-output entity :output] [])
        (when out
          (doseq [line output]
            (.write out line))
          (.write out "\r\n")
          (.write out (telnet-state-prompt (get-in components [:telnet-state entity])))
          (.flush out))))
    @*components))

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
                                 (.offer effect-queue {:type :telnet-connection :entity entity :out out})
                                 (while true
                                   (let [line (.readLine in)]
                                     (println (str "Received: " line))
                                     (.offer effect-queue {:type :telnet-input :entity entity :line line}))))))])))))

(defn -main
  []
  (reset! *systems
          [
           {:f handle-add-component
            :f-arg :types->entities->component
            :type :effect-handler
            :name "handle-add-component"
            :handle-effects #{:add-component}
            :uses-components :all
            :updates-components :all}
           {:f handle-telnet-connection
            :f-arg :types->entities->component
            :type :effect-handler
            :name "handle-telnet-connection"
            :handle-effects #{:telnet-connection}
            :uses-components [:telnet-state :telnet-input :telnet-output]
            :updates-components [:telnet-state :telnet-input :telnet-output]}
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
            :uses-components [:telnet-state :telnet-input :telnet-output :command-queue]
            :updates-components [:telnet-state :telnet-input :telnet-output :command-queue]}
           {:f process-command-queue
            :f-arg :types->entities->component
            :type :periodic
            :name "process-command-queue"
            :pulses 1
            :uses-components :all
            :updates-components :all}
           {:f process-player-perceptions
            :f-arg :types->entities->component
            :type :periodic
            :name "process-player-perceptions"
            :pulses 1
            :uses-components [:perceptor :telnet-state :telnet-output]
            :updates-components [:perceptor :telnet-output]}
           {:f write-telnet-outputs
            :f-arg :types->entities->component
            :type :periodic
            :name "write-telnet-outputs"
            :pulses 1
            :uses-components [:telnet-output :telnet-state]
            :updates-components [:telnet-output]}
           ])
  ;; uncomment for performance testing
  #_(dotimes [n 100000]
    (let [entity (new-entity!)]
      (.offer effect-queue {:type :add-component :entity entity :component :lifetime-tracker :data {:pulses 0}})
      (.offer effect-queue {:type :telnet-connection :entity entity :out nil})))
  (let [config (edn/read-string (slurp (io/resource "server-config.edn")))
        telnet-thread (Thread/startVirtualThread
                       (fn telnet-handler[]
                         (run-telnet-server config)))
        game-thread (Thread/startVirtualThread
                     (fn game-handler[]
                       (run-game-server config)))]
    (.join game-thread)))
