(ns ripmud.server
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.set :as set]
            [ripmud.job :as job]))

(def millis-per-pulse 250)

(def *next-entity (ref 0))

(defn new-entity!
  []
  (dosync
   (let [entity (alter *next-entity inc)]
     entity)))

(defn make-effect-handler-job-runner
  [handle-effect]
  {:f (fn effect-handler-job-runner-f [effects
                                       {:keys [f] :as job}
                                       job-arg]
        (when (seq effects)
          (let [job-result (reduce f job-arg effects)]
            [job-result nil])))
   :uses #{[:effects handle-effect]}
   :updates #{[:effects handle-effect]}})

(defn make-event-listener-job-runner
  [handle-events]
  {:f (fn event-listener-job-runner-f [events
                                       {:keys [f] :as job}
                                       job-arg]
        (let [events-handling (filter #(= handle-events (:type %)) events)]
          (when (seq events-handling)
            (let [job-result (reduce f job-arg events-handling)]
              [job-result nil]))))
   :uses #{[:events]}})

(defn make-periodic-job-runner
  [pulses]
  {:f (fn periodic-job-runner-f [pulse
                                 {:keys [f] :as job}
                                 job-arg]
        (when (zero? (mod pulse pulses))
          [(f job-arg)]))
   :uses #{[:pulse]}})

;;; Effects are for things that come about from outside a system, like input
;;; from a telnet socket.  Once a system handles an effect, it is discarded.
(def effect-queue (java.util.concurrent.ConcurrentLinkedQueue.))

(defn slurp-effects
  [effects]
  (if-let [effect (.poll effect-queue)]
    (recur (update effects (:type effect) conj effect))
    effects))

(def s-slurp-effects
  {:id :slurp-effects
   :f slurp-effects
   :uses #{[:effects]}
   :updates #{[:effects]}})

(def s-increment-pulse
  {:id :increment-pulse
   :f inc
   :uses #{[:pulse]}
   :updates #{[:pulse]}})

(defn add-entity-with-components
  [components entity entity-components]
  (reduce (fn [components [component-key component]]
            (assoc-in components [component-key entity] component))
          components
          entity-components))

(defn run-game-server
  [config systems]
  (dorun (map job/validate systems))
  (let [execution-plan (job/jobs->execution-plan systems)
        start-entity (new-entity!)
        start-entity-components {:desc {:name "The Void"}
                                 :contains []}]
    #_(pprint execution-plan)
    (loop [game-state {:pulse 0
                       :components (add-entity-with-components {} start-entity start-entity-components)
                       :effects {}
                       :start-entity start-entity}]
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

(def s-handle-add-component
  {:id :handle-add-component
   :f handle-add-component
   :runner (make-effect-handler-job-runner :add-component)
   :uses #{[:components]}
   :updates #{[:components]}})

(defn handle-telnet-input
  "Takes telnet input effects and puts them into the correct entity's component."
  [telnet-inputs effect]
  (let [{:keys [entity line]} effect
        telnet-input (get telnet-inputs entity)]
    (assoc telnet-inputs entity (update telnet-input :input conj line))))

(def s-handle-telnet-input
  {:id :handle-telnet-input
    :f handle-telnet-input
    :runner (make-effect-handler-job-runner :telnet-input)
    :uses #{[:components :telnet-input]}
    :updates #{[:components :telnet-input]}})

(defn player?
  "Returns true if the entity is a player."
  [entity components]
  (not (nil? (get-in components [:player entity]))))

(defprotocol TelnetState
  (telnet-state-prompt [state])
  (telnet-state-entered [state telnet-input telnet-output])
  (telnet-state-input [state entity telnet-input telnet-output command-queue])
  (telnet-state-left [state telnet-input telnet-output]))

(defn target-entity-id
  [descs arg-str]
  (if-let [target (parse-long arg-str)]
    target
    (let [arg-str (str/lower-case arg-str)
          [target-n kw] (if-let [idx (str/index-of arg-str ".")]
                          [(dec (parse-long (str/trim (subs arg-str 0 idx)))) (subs arg-str (inc idx))]
                          [0 arg-str])
          targets (map first (filter (fn [[entity desc]]
                                       (str/starts-with? (:name desc) kw))
                                     descs))]
      (if (< -1 target-n (count targets))
        (nth targets target-n)
        nil))))

(def cmd-say
  {:name "say"
   :restrictions []
   :args :arg-str
   :f (fn [components actor arg-str]
        (update-in components [:perceptor actor :perceptions] concat [{:act :say :actor actor :message arg-str}]))})

(def cmd-shout
  {:name "shout"
   :restrictions []
   :args :arg-str
   :f (fn [components actor arg-str]
        (reduce (fn [components target]
                  (update-in components [:perceptor target :perceptions] conj {:act :shout :actor actor :message arg-str}))
                components
                (keys (:player components))))})

(def cmd-look
  {:name "look"
   :restrictions []
   :args :arg-str
   :f (fn cmd-look[components actor arg-str]
        (let [location (get-in components [:location actor])]
          (update-in components [:perceptor actor :perceptions] conj {:act :look :actor actor :location location})))})

(def cmd-jimmie
  {:name "jimmie"
   :restrictions [player?]
   :args :none
   :f (fn [components actor arg-str]
        (update-in components [:telnet-output actor :output] concat ["JIMMMIEEEE! JIMMIE JIMMIE JIMMIESON!\r\n"]))})

(def cmd-components
  {:name "components"
   :restrictions [player?]
   :args :arg-list
   :f (fn [components actor arg-str]
        (if-let [target (if (seq arg-str)
                          (target-entity-id (:desc components) (first arg-str))
                          actor)]
          (let [output (reduce (fn [components-str [component-key components]]
                                 (if-let [component (get components target)]
                                   (str components-str component-key ": " (get components target) "\r\n")
                                   components-str))
                               ""
                               components)
                output (str ":entity " target "\r\n" output)]
            (update-in components [:telnet-output actor :output] concat [output]))
          (update-in components [:telnet-output actor :output] concat [(str "Could not find '" (first arg-str) "'\r\n")])))})

(def cmd-quit
  {:name "quit"
   :restrictions [player?]
   :args :str-cmd
   :f (fn [components actor str-cmd]
        (update-in components [:telnet-output actor :output] concat ["Quit not implemented, you're stuck here forever!\r\n"]))})

(def command-table
  [cmd-say
   cmd-shout
   cmd-look
   cmd-jimmie
   cmd-components])

(defn can-use-cmd?
  [cmd entity components]
  (every? #(% entity components)
          (:restrictions cmd)))

(defrecord TelnetStatePlaying [name]
  TelnetState
  (telnet-state-prompt [state] "> ")
  (telnet-state-entered [state telnet-input telnet-output]
    [state telnet-input (update telnet-output :output concat [(str "Welcome " name "!\r\n")])])
  (telnet-state-input [state entity {:keys [input] :as telnet-input} telnet-output command-queue]
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
  (telnet-state-input [state entity {:keys [input] :as telnet-input} telnet-output command-queue]
    (if-let [line (first input)]
      [(TelnetStatePlaying. line)
       (update telnet-input :input rest)
       telnet-output
       command-queue
       [{:type :playing :entity entity :name line}]]
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

(def s-handle-telnet-connection
  {:id :handle-telnet-connection
   :f handle-telnet-connection
   :runner (make-effect-handler-job-runner :telnet-connection)
   :uses #{[:components :telnet-state]
           [:components :telnet-input]
           [:components :telnet-output]
           [:components :player]}
   :updates #{[:components :telnet-state]
              [:components :telnet-input]
              [:components :telnet-output]
              [:components :player]}})

(defn process-telnet-inputs
  "Takes input from the telnet-input component and processes the command, writing any output to the telnet-output component."
  [components]
  (let [*components (atom components)
        *events (atom [])]
    (doseq [[entity telnet-input] (:telnet-input components)]
      (when (seq (:input telnet-input))
        (let [telnet-output (get-in components [:telnet-output entity])
              telnet-state (get-in components [:telnet-state entity])
              command-queue (get-in components [:command-queue entity])]
          (let [[telnet-state' telnet-input' telnet-output' command-queue' events'] (telnet-state-input telnet-state entity telnet-input telnet-output command-queue)
                [telnet-state' telnet-input' telnet-output'] (if (= (type telnet-state) (type telnet-state'))
                                                               [telnet-state' telnet-input' telnet-output']
                                                               ;; call entered
                                                               (telnet-state-entered telnet-state' telnet-input' telnet-output'))]

            (swap! *events into events')
            (swap! *components assoc-in [:telnet-state entity] telnet-state')
            (swap! *components assoc-in [:telnet-input entity] telnet-input')
            (swap! *components assoc-in [:telnet-output entity] telnet-output')
            (swap! *components assoc-in [:command-queue entity] command-queue')))))
    {:components @*components
     :events @*events}))

(def s-process-telnet-inputs
  {:id :process-telnet-inputs
    :f process-telnet-inputs
    :uses #{[:components :telnet-state]
            [:components :telnet-input]
            [:components :telnet-output]
            [:components :command-queue]}
    :updates #{[:components :telnet-state]
               [:components :telnet-input]
               [:components :telnet-output]
               [:components :command-queue]}
    :appends #{[:events]}})

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

(def s-process-command-queue
  {:id :process-command-queue
    :f process-command-queue
    :uses #{[:components]}
    :updates #{[:components]}})

(defn actor-name
  [actor entity components]
  (if (= actor entity)
    "You"
    (str/capitalize (get-in components [:desc actor :name]))))

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
                (swap! *components update-in [:telnet-output entity :output] concat [(str (actor-name actor entity components) " says \"" (:message action) "\"\r\n")])

                :shout
                (swap! *components update-in [:telnet-output entity :output] concat [(str (actor-name actor entity components) " shouts \"" (:message action) "\"\r\n")])

                :look
                (let [location (get-in components [:location entity])
                      contains (get-in components [:contains location])
                      location-name (get-in components [:desc location :name])
                      entities-str (reduce (fn [output other]
                                             (if (= other entity)
                                               output
                                               (str output "You see " (get-in components [:desc other :name]) " standing here.\r\n")))
                                           ""
                                           contains)
                      location-desc (str location-name "\r\n" entities-str)]
                  (swap! *components update-in [:telnet-output entity :output] into [location-desc]))

                (throw (ex-info (str "Unknown act: " act) {:act act}))))))))
    @*components))

(def s-process-player-perceptions
  {:id :process-player-perceptions
    :f process-player-perceptions
    :uses #{[:components]}
    :updates #{[:components :perceptor]
               [:components :telnet-output]}})

(defn on-playing-event
  [{:keys [start-entity components] :as state}
   {:keys [entity name]}]
  (-> components
      (assoc-in [:desc entity] {:name name})
      (assoc-in [:location entity] start-entity)
      (assoc-in [:contains entity] [])
      (update-in [:contains start-entity] into [entity])))

(def s-on-playing-event
  {:id :on-playing-event
   :f on-playing-event
   :runner (make-event-listener-job-runner :playing)
   :uses #{[:start-entity]
           [:components :desc]
           [:components :location]
           [:components :contains]}
   :updates #{[:components :desc]
              [:components :location]
              [:components :contains]}})

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

(def s-process-npc-perceptions
  {:id :process-npc-perceptions
    :f process-npc-perceptions
    :uses #{[:components :perceptor]
            [:components :player]
            [:components :command-queue]}
    :updates #{[:components :perceptor]
               [:components :command-queue]}})

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

(def s-write-telnet-outputs
  {:id :write-telnet-outputs
    :f write-telnet-outputs
    :uses #{[:components :telnet-output]
            [:components :telnet-state]}
    :updates #{[:components :telnet-output]}})

(defn update-lifetimes
  [entity->lifetimes]
  (update-vals entity->lifetimes
               (fn [lifetime-tracker]
                 (update lifetime-tracker :pulses inc))))

(def s-update-lifetimes
  {:id :update-lifetimes
   :f update-lifetimes
   :uses #{[:components :lifetime-tracker]}
   :updates #{[:components :lifetime-tracker]}})

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

(def systems
  [
   s-slurp-effects
   s-increment-pulse
   s-handle-add-component
   s-handle-telnet-connection
   s-update-lifetimes
   s-handle-telnet-input
   s-process-telnet-inputs
   s-process-command-queue
   s-process-player-perceptions
   s-process-npc-perceptions
   s-write-telnet-outputs

   ;; event listeners go at the end
   s-on-playing-event
   {:id :clear-events :f (fn clear-events[_] []) :uses #{[:events]} :updates #{[:events]}}
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
