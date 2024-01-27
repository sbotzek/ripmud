;;;; Contains functions for running and automatically parallelizing a list of jobs.

(ns ripmud.job
  (:require [clojure.set :as set]))

(defprotocol JobRunner
  "A protocol for customizing how jobs are run."
  (run [this runner-arg job job-arg])
  (uses [this])
  (updates [this]))

(defrecord SimpleJobRunner []
  JobRunner
  (run [this _runner-arg
        {:keys [f] :as job} job-arg]
    [(f job-arg)])
  (uses [this]
    #{})
  (updates [this]
    #{}))

(def *default-job-runner (atom (SimpleJobRunner.)))

(defn f-arg
  "Returns the args a function needs based upon what it uses."
  [uses state]
  ;; TODO: calculating the 'shape' could be a 1 time thing
  (loop [arg-shape (reduce (fn [sofar arg]
                       (assoc-in sofar arg {}))
                     {}
                     uses)
        arg (reduce (fn [sofar arg]
                       (assoc-in sofar arg (get-in state arg)))
                     {}
                     uses)]
    (if (= 1 (count arg-shape))
      (recur (first (vals arg-shape))
             (first (vals arg)))
      arg)))

(defn update-state
  "Updates the state from the result based upon the keys in updates."
  [state result updates]
  ;; TODO: calculating the 'shape' could be a 1 time thing
  (loop [result-shape (reduce (fn [sofar arg]
                                (assoc-in sofar arg {}))
                              {}
                              updates)
         result-path []]
    (if (= 1 (count result-shape))
      (recur
       (first (vals result-shape))
       (conj result-path (first (keys result-shape))))
      (let [result (if (seq result-path)
                     (assoc-in {} result-path result)
                     result)]
        (reduce (fn [sofar arg]
                  (assoc-in sofar arg (get-in result arg)))
                state
                updates)))))

(defn merge-keys
  "Merges a list of keps from one map into another."
  [state state' keys]
  (reduce (fn [sofar key]
            (assoc-in sofar key (get-in state' key)))
          state
          keys))

(defn validate
  "Validates a job."
  [{:keys [runner id] :as job}]
  (cond
    (and (not (= (:uses job) (:updates job)))
         (not (set/subset? (:updates job) (:uses job))))
    (throw (ex-info (str "Job " id " 'updates' is not a subset of 'uses'.") {:job job}))

    (and runner
         (not (= (uses runner) (updates runner)))
         (not (set/subset? (updates runner) (uses runner))))
    (throw (ex-info (str "Job " id "'s runner 'updates' is not a subset of 'uses'.") {:job job}))))

(defn overlapping?
  "Returns true if two jobs incompatibly overlap in their updates/uses."
  [job1 job2]
  (let [runner1 (or (:runner job1) @*default-job-runner)
        runner2 (or (:runner job2) @*default-job-runner)
        uses1 (into (:uses job1) (uses runner1))
        uses2 (into (:uses job2) (uses runner2))
        updates1 (into (:updates job1) (updates runner1))
        updates2 (into (:updates job2) (updates runner2))]
    (or (some (fn [update]
                (some (fn [uses]
                        (let [c (min (count update) (count uses))]
                          (= (take c uses) (take c update))))
                      uses2))
              updates1)
        (some (fn [update]
                (some (fn [uses]
                        (let [c (min (count update) (count uses))]
                          (= (take c uses) (take c update))))
                      uses1))
              updates2))))

(defn in-dependency-graph?
  "Returns true if the candidate is in the dependency graph for root."
  [dependency-graph root candidate]
  (loop [visited #{}
         [visit & queued] [root]]
    (if-not visit
      false
      (let [dependencies (get dependency-graph visit)]
        (if (get dependencies candidate)
          true
          (let [new-visits (set/difference dependencies visited)
                visited' (set/union visited new-visits)]
            (recur visited' (concat queued new-visits))))))))

(defn common-roots
  "Returns the common roots of a list of nodes"
  [dependency-graph all for]
  (loop [[candidate & candidates] (reverse (concat (take-while #(not (some (fn [s] (= s %)) for)) all)
                                                  all))
         roots #{}]
    (cond
      (not candidate)
      roots

      (and (every? #(not (in-dependency-graph? dependency-graph % candidate)) roots)
           (every? #(or (in-dependency-graph? dependency-graph % candidate)
                        (= % candidate)) for))
      (recur candidates (conj roots candidate))

      :else
      (recur candidates roots))))

(defn nodes-in-dependency-graph
  "Returns the nodes in root's dependency graph ending at 'until' (exclusive)."
  [dependency-graph root until]
  (loop [subtree [root]
         visited (conj (into #{} until) root)
         [visit & queued] [root]]
    (if-not visit
      subtree
      (let [dependencies (get dependency-graph visit)
            new-visits (set/difference dependencies visited)
            subtree' (into subtree new-visits)
            visited' (set/union visited new-visits)
            queue' (concat queued new-visits)]
        (recur subtree' visited' queue')))))

(defn jobs->dependency-graph
  "Returns a dependency graph for a list of jobs."
  [jobs]
  (loop [[job & jobs'] jobs
         visited-jobs []
         graph {}]
    (let [dependencies (filter #(overlapping? job %) visited-jobs)
          dependencies-without-redundancies (filter (fn not-in-dependency-dependency-graph?[dependency]
                                                      (not (some (fn in-dependency-graph?-2[dependency2]
                                                                   (in-dependency-graph? graph (:id dependency2) (:id dependency)))
                                                                 dependencies)))
                                                    dependencies)
          graph' (assoc graph (:id job) (set (map :id dependencies-without-redundancies)))]
      (if (seq jobs')
        (recur jobs'
               (conj visited-jobs job)
               graph')
        graph'))))

(defn run-job
  "Runs a job."
  [state {:keys [runner] :or {runner @*default-job-runner} :as job}]
  (try
    (let [start-time (System/currentTimeMillis)
          job-arg (f-arg (:uses job) state)
          runner-arg (f-arg (uses runner) state)
          results (run runner runner-arg job job-arg)
          state' (cond-> state
                   (> (count results) 1)
                   (update-state (second results) (updates runner))

                   (> (count results) 0)
                   (update-state (first results) (:updates job)))]
      #_(locking *out*
        #_(println "*****************************************************************" (:id job) "*****************************************************************")
        #_(println "Job" (:id job) "total ms:" (- (System/currentTimeMillis) start-time))
        #_(println "  Job" (:id job) " state" state)
        #_(println "  Job" (:id job) "state'" state'))
      state')
    (catch Exception e
      (throw (ex-info (str "Error running job " (:id job)) {:job job :exception e :state state})))))

(defn jobs->execution-plan
  "Returns an execution plan for a list of jobs."
  [jobs]
  (dorun (map validate jobs))
  (let [dependency-graph (jobs->dependency-graph jobs)
        job-ids (map :id jobs)
        start-queue (java.util.concurrent.LinkedBlockingQueue.)
        start-node {:job {:id ::start-node :f identity :updates #{} :uses #{}}
                    :dependency->queue {:whatever start-queue}
                    :output-queues []}
        id->job (zipmap (map :id jobs) jobs)]
    (loop [nodes [start-node]
           visited-jobs []
           [job & jobs'] jobs]
      (if job
        (let [dependencies (get dependency-graph (:id job))
              dependencies (if (seq dependencies)
                             dependencies
                             #{::start-node})
              dependency->queue (reduce (fn [sofar dependency]
                                          (assoc sofar dependency (java.util.concurrent.LinkedBlockingQueue.)))
                                        {}
                                        dependencies)
              nodes-with-output-queues (map (fn [node]
                                              (if-let [queue (get dependency->queue (:id (:job node)))]
                                                (assoc node :output-queues (conj (:output-queues node) queue))
                                                node))
                                            nodes)
              node {:job job
                    :dependency->queue dependency->queue
                    :output-queues []}
              nodes' (conj nodes-with-output-queues node)]
          (recur nodes'
                 (conj visited-jobs job)
                 jobs'))
        (let [;; we need to hook leaf nodes up to output queues so we can get the final output
              leaf-nodes (filter #(empty? (:output-queues %)) nodes)
              leaf-jobs->queue (reduce (fn [sofar node]
                                            (assoc sofar (:id (:job node)) (java.util.concurrent.LinkedBlockingQueue.)))
                                          {}
                                          leaf-nodes)
              nodes-with-leaf-output-queues (map (fn [node]
                                                   (if-let [queue (get leaf-jobs->queue (:id (:job node)))]
                                                     (assoc node :output-queues (conj (:output-queues node) queue))
                                                     node))
                                                 nodes)
              ;; add an end node
              end-output-queue (java.util.concurrent.LinkedBlockingQueue.)
              end-node {:job {:id ::end-node :f identity :updates #{} :uses #{}}
                        :dependency->queue leaf-jobs->queue
                        :output-queues [end-output-queue]}
              nodes-with-end-node (conj nodes-with-leaf-output-queues end-node)
              ;; create all the functions & threads & add the start queues
              nodes-with-thread (mapv (fn [{:keys [job dependency->queue output-queues] :as node}]
                                        (let [f (fn node-f[]
                                                  (let [*visited-job-ids (atom [])
                                                        state (reduce (fn merge-job-input[state-acc [job-id queue]]
                                                                        (let [job-state (.take queue)]
                                                                          (swap! *visited-job-ids conj job-id)
                                                                          (if-not state-acc
                                                                            job-state
                                                                            (let [roots (common-roots dependency-graph job-ids @*visited-job-ids)
                                                                                  in-graph (nodes-in-dependency-graph dependency-graph job-id roots)
                                                                                  in-graph-jobs (map id->job in-graph)
                                                                                  in-graph-updates (into #{} (mapcat (fn job-updates[{:keys [runner] :as job}]
                                                                                                                       (cond-> (:updates job)
                                                                                                                         runner
                                                                                                                         (concat (updates runner))))
                                                                                                                     in-graph-jobs))]
                                                                              (merge-keys state-acc job-state in-graph-updates)))))
                                                                      nil
                                                                      dependency->queue)
                                                        state' (run-job state job)]
                                                    (doseq [queue output-queues]
                                                      (.offer queue state'))
                                                    (recur)))]
                                          {:job job
                                           :f f
                                           :dependencies (keys dependency->queue)
                                           :thread #_(.start (Thread. f)) (Thread/startVirtualThread f)}))
                                      nodes-with-end-node)]
          {:nodes nodes-with-thread
           :start-queue start-queue
           :end-queue end-output-queue})))))

(defn execute-step
  "Runs the execution plan for a single step, returning the result."
  [execution-plan state]
  (.offer (:start-queue execution-plan) state)
  (.take (:end-queue execution-plan)))
