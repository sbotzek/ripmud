(ns ripmud.server
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.set :as set]
            [ripmud.job :as job]))

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

(defn slurp-effects
  [effects]
  (if-let [effect (.poll effect-queue)]
    (recur (update effects (:type effect) conj effect))
    effects))

(defn run-game-server
  [config systems]
  (dorun (map job/validate systems))
  (let [execution-plan (job/jobs->execution-plan systems)]
    #_(pprint execution-plan)
    (loop [game-state {:pulse 0
                       :components {}
                       :effects {}}]
      #_(pprint game-state)
      #_(println "game-state" game-state)
      (let [start-time (System/currentTimeMillis)]
        (let [game-state' (job/execute-step execution-plan game-state)]
          (let [elapsed-time (- (System/currentTimeMillis) start-time)]
            (println "Sleeping For" (- millis-per-pulse elapsed-time) "ms")
            (when (< elapsed-time millis-per-pulse)
              (Thread/sleep (- millis-per-pulse elapsed-time)))
            (recur game-state')))))))

(defn handle-add-component
  "Handles add component effect by adding the component to the entity"
  [components {:keys [entity component data]}]
  (assoc-in components [component entity] data))

(defn handle-telnet-input
  "Takes telnet input effects and puts them into the correct entity's component."
  [telnet-inputs effect]
  (let [{:keys [entity line]} effect
        telnet-input (get telnet-inputs entity)]
    (assoc telnet-inputs entity (update telnet-input :input conj line))))

(defn player?
  "Returns true if the entity is a player."
  [entity components]
  (not (nil? (get-in components [:player entity]))))

(defprotocol TelnetState
  (telnet-state-prompt [state])
  (telnet-state-entered [state telnet-input telnet-output])
  (telnet-state-input [state telnet-input telnet-output command-queue])
  (telnet-state-left [state telnet-input telnet-output]))

(def command-table
  [{:name "say"
    :restrictions []
    :args :arg-str
    :f (fn [components actor arg-str]
         (update-in components [:perceptor actor :perceptions] concat [{:act :say :actor actor :message arg-str}]))}
   {:name "shout"
    :restrictions []
    :args :arg-str
    :f (fn [components actor arg-str]
         (reduce (fn [components target]
                   (update-in components [:perceptor target :perceptions] conj {:act :shout :actor actor :message arg-str}))
                 components
                 @*entities))}
   {:name "jimmie"
    :restrictions [:player]
    :args :none
    :f (fn [components actor arg-str]
         (update-in components [:telnet-output actor :output] concat ["JIMMMIEEEE! JIMMIE JIMMIE JIMMIESON!\r\n"]))}

   {:name "components"
    :restrictions [:player]
    :args :arg-list
    :f (fn [components actor arg-str]
         (let [target (if (seq arg-str)
                        (parse-long (first arg-str))
                        actor)
               output (reduce (fn [components-str [component-key components]]
                   (if-let [component (get components target)]
                     (str components-str component-key ": " (get components target) "\r\n")
                     components-str))
                 ""
                 components)]
            (update-in components [:telnet-output actor :output] concat [output])))}
   {:name "quit"
    :restrictions [:player]
    :args :str-cmd
    :f (fn [components actor str-cmd]
         (update-in components [:telnet-output actor :output] concat ["Quit not implemented, you're stuck here forever!\r\n"]))}])

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
        components' (cond-> components
                      out (assoc-in [:player entity] {})
                      true (assoc-in [:telnet-state entity] telnet-state)
                      true (assoc-in [:telnet-input entity] telnet-input)
                      true (assoc-in [:telnet-output entity] telnet-output))]
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
    (doseq [entity (keys (:player components))]
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
                :shout
                (if (= actor entity)
                  (swap! *components update-in [:telnet-output entity :output] concat [(str "You shout \"" (:message action) "\"\r\n")])
                  (swap! *components update-in [:telnet-output entity :output] concat [(str actor " shouts \"" (:message action) "\"\r\n")]))
                (throw (ex-info (str "Unknown act: " act) {:act act}))))))))
    @*components))

(defn process-npc-perceptions
  "Takes perceptions from the perceptor component and writes them to the telnet-output component."
  [components]
  (let [*perceptors (atom (transient {}))]
    (doseq [[entity perceptor] (:perceptor components)]
      (if (player? entity components)
        (swap! *perceptors assoc! entity perceptor)
        (when-let [perceptions (:perceptions perceptor)]
          (doseq [{:keys [act actor] :as action} perceptions]
              (case act
                true
                #_(println "NPC" entity "perceived" act "from" actor))))))
    (assoc components :perceptor (persistent! @*perceptors))))

(defn write-telnet-outputs
  "Takes output from the telnet-output component and writes it to the socket."
  [components]
  (let [*telnet-outputs' (atom (:telnet-output components))]
    (doseq [[entity {:keys [output out] :as telnet-output}] (:telnet-output components)]
      (when (seq output)
        (swap! *telnet-outputs' assoc-in [entity :output] [])
        (when out
          (doseq [line output]
            (.write out line))
          (.write out "\r\n")
          (.write out (telnet-state-prompt (get-in components [:telnet-state entity])))
          (.flush out))))
    @*telnet-outputs'))

(defn update-lifetimes
  [entity->lifetimes]
  (update-vals entity->lifetimes
               (fn [lifetime-tracker]
                 (update lifetime-tracker :pulses inc))))

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

(defrecord EffectHandlerJobRunner [handle-effect]
  job/JobRunner
  (run [{:keys [handle-effect] :as this}
        effects
        {:keys [f] :as job}
        job-arg]
    (let [job-result (reduce f job-arg effects)]
      [job-result nil]))
  (uses [this]
    #{[:effects handle-effect]})
  (updates [this]
    #{[:effects handle-effect]}))

(defrecord PeriodicJobRunner [pulses]
  job/JobRunner
  (run [this pulse
        {:keys [f] :as job} job-arg]
    (if (zero? (mod pulse (:pulses this)))
      [(f job-arg)]
      [job-arg]))
  (uses [this]
    #{[:pulse]})
  (updates [this]
    #{}))

(def systems
  [
   {:id :increment-pulse
    :f inc
    :uses #{[:pulse]}
    :updates #{[:pulse]}}
   {:id :slurp-effects
    :f slurp-effects
    :uses #{[:effects]}
    :updates #{[:effects]}}
   {:id :handle-add-component
    :f handle-add-component
    :runner (EffectHandlerJobRunner. :add-component)
    :uses #{[:components]}
    :updates #{[:components]}}
   {:id :handle-telnet-connection
    :f handle-telnet-connection
    :runner (EffectHandlerJobRunner. :telnet-connection)
    :uses #{[:components :telnet-state]
            [:components :telnet-input]
            [:components :telnet-output]
            [:components :player]}
    :updates #{[:components :telnet-state]
               [:components :telnet-input]
               [:components :telnet-output]
               [:components :player]}}
   {:id :update-lifetimes
    :f update-lifetimes
    :uses #{[:components :lifetime-tracker]}
    :updates #{[:components :lifetime-tracker]}}
   {:id :handle-telnet-input
    :f handle-telnet-input
    :runner (EffectHandlerJobRunner. :telnet-input)
    :uses #{[:components :telnet-input]}
    :updates #{[:components :telnet-input]}}
   {:id :process-telnet-inputs
    :f process-telnet-inputs
    :uses #{[:components :telnet-state]
            [:components :telnet-input]
            [:components :telnet-output]
            [:components :command-queue]}
    :updates #{[:components :telnet-state]
               [:components :telnet-input]
               [:components :telnet-output]
               [:components :command-queue]}}
   {:id :process-command-queue
    :f process-command-queue
    :uses #{[:components]}
    :updates #{[:components]}}
   {:id :process-player-perceptions
    :f process-player-perceptions
    :uses #{[:components :player]
            [:components :perceptor]
            [:components :telnet-state]
            [:components :telnet-output]}
    :updates #{[:components :perceptor]
               [:components :telnet-output]}}
   {:id :process-npc-perceptions
    :f process-npc-perceptions
    :uses #{[:components :perceptor]
            [:components :player]
            [:components :command-queue]}
    :updates #{[:components :perceptor]
               [:components :command-queue]}}
   {:id :write-telnet-outputs
    :f write-telnet-outputs
    :uses #{[:components :telnet-output]
            [:components :telnet-state]}
    :updates #{[:components :telnet-output]}}
   ])

(defn -main
  []
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
                       (run-game-server config systems)))]
    (.join game-thread)))
