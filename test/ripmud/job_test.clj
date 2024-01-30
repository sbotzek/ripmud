(ns ripmud.job-test
  (:require [ripmud.job :as job]
            [clojure.test :refer [are deftest is testing]]))

(def complex-test-jobs
  [{:id :a
    :uses #{[:a]}
    :updates #{[:a]}}
   {:id :b
    :uses #{[:a] [:b]}
    :updates #{[:b]}}
   {:id :c
    :uses #{[:c]}
    :updates #{[:c]}}
   {:id :d
    :uses #{[:c] [:d]}
    :updates #{[:d]}}
   {:id :e
    :uses #{[:e]}
    :updates #{[:e]}}
   {:id :b+d
    :uses #{[:b] [:c] [:d]}
    :updates #{[:b] [:c] [:d]}}
   {:id :a+b+e :uses #{[:a] [:b] [:e]}
    :updates #{[:a] [:e]}}
   {:id :c2
    :uses #{[:c]}
    :updates #{[:c]}}
   {:id :b+c+e
    :uses #{[:b] [:c] [:e]}
    :updates #{[:e]}}])

(defrecord TestJobRunner[f uses updates]
  job/JobRunner
  (run [this runner-arg job job-arg]
    [((:f job) job-arg) ((:f this) runner-arg)])
  (uses [this]
    (:uses this))
  (updates [this]
    (:updates this)))


(deftest test-f-arg
  (testing "Only returns keys that are used by the job."
    (is (= {:a 1 :b 3}
           (job/f-arg #{[:a] [:b]}
                      {:a 1 :b 3 :c 4}))))
  (testing "Removes unecessary nesting"
    (is (= {:b 1 :c 2}
           (job/f-arg #{[:a :b] [:a :c]}
                      {:a {:b 1 :c 2} :d 3})))))

(deftest test-update-state
  (testing "Only updates keys that are used in the update."
    (is (= {:a 4 :b 5 :c 3}
           (job/update-state {:a 1 :b 2 :c 3}
                             {:a 4 :b 5 :c 6}
                             #{[:a] [:b]}))))
  (testing "Re-adds necessary nesting when updating."
    (is (= {:a {:b 3 :c 4}}
           (job/update-state {:a {:b 1 :c 2}}
                             {:b 3 :c 4}
                             #{[:a :b] [:a :c]}))))
  (testing "Appends goes to another spot"
    (is (= {:a {:b 3 :c 4 :f []}
            :ripmud.job/appends {:a {:f {:jobkey [3 4 5]}}}}
           (job/update-state {:a {:b 1 :c 2 :f []}}
                             {:b 3 :c 4 :f [3 4 5]}
                             #{[:a :b] [:a :c]}
                             #{[:a :f]}
                             :jobkey)))))

(deftest test-merge-keys
  (testing "Ignores keys that aren't being merged"
    (is (= {:a 4 :b 5 :c 3}
           (job/merge-keys {:a 1 :b 2 :c 3}
                           {:a 4 :b 5 :c 6}
                           [[:a] [:b]]
                           []))))
  (testing "Nested keys works"
    (is (= {:a {:b 4 :c 2} :d 3}
           (job/merge-keys {:a {:b 1 :c 2} :d 3}
                           {:a {:b 4 :c 5} :d 6}
                           [[:a :b]]
                           []))))
  (testing "Ignores appends that aren't being merged"
    (is (= {:ripmud.job/appends {:a {:b {0 [3 4] 1 [5 6]}}}}
           (job/merge-keys {}
                           {:ripmud.job/appends {:a {:b {0 [3 4] 1 [5 6]}
                                                     :c {0 [20 30] 1 [40 50]}}}}
                           []
                           [[:a :b]])))))

(deftest test-validate
  (testing "Job validates when 'uses' same as 'updates'."
    (is (job/validate {:id :test
                            :uses #{[:a] [:b] [:c]}
                            :updates #{[:a] [:b] [:c]}})))
  (testing "Job validates when 'uses' superset of 'updates'."
    (is (job/validate {:id :test
                            :uses #{[:a] [:b] [:c] [:d]}
                            :updates #{[:a] [:b] [:c]}})))
  (testing "Cannot both 'use' and 'append' the same key."
    (is (thrown? Exception (job/validate {:id :test
                                          :uses #{[:a] [:b] [:c]}
                                          :appends #{[:a] [:c] [:d]}
                                          :updates #{}}))))
  (testing "Cannot both 'use' and 'update' the same key."
    (is (thrown? Exception (job/validate {:id :test
                                          :uses #{[:a] [:b] [:c]}
                                          :appends #{[:a] [:c] [:d]}
                                          :updates #{[:a] [:b] [:c]}}))))
  (testing "Job doesn't validate when 'uses' is not a superset of 'updates'."
    (is (thrown? Exception (job/validate {:id :test
                                          :uses #{[:a] [:b] [:c]}
                                          :updates #{[:a] [:c] [:d]}}))))
  (testing "Job validates when 'uses' is a prefix of 'updates'."
    (is (job/validate {:id :testxx
                       :uses #{[:a]}
                       :updates #{[:a :b]}})))
  (testing "Job doesn't validate when 'updates' is a prefix of 'uses'."
    (is (thrown? Exception (job/validate {:id :test
                                          :uses #{[:a :b]}
                                          :updates #{[:a]}}))))
  (testing "Runner validates when 'uses' same as 'updates'."
    (is (job/validate {:id :test
                            :uses #{}
                            :updates #{}
                            :runner (TestJobRunner. inc #{[:a] [:b] [:c]} #{[:a] [:b] [:c]})})))
  (testing "Runner validates when 'uses' superset of 'updates'."
    (is (job/validate {:id :test
                            :uses #{}
                            :updates #{}
                            :runner (TestJobRunner. inc #{[:a] [:b] [:c] [:d]} #{[:a] [:b] [:c]})})))
  (testing "Runner doesn't validate when 'uses' is not a superset of 'updates'."
    (is (thrown? Exception (job/validate {:id :test
                                          :uses #{}
                                          :updates #{}
                                          :runner (TestJobRunner. inc #{[:a] [:b] [:c]} #{[:a] [:b] [:d]})})))))


(deftest test-apply-appends-to-specific-keys
  (testing "Basic example"
    (is (= {:a [1 2 3 4]
            :ripmud.job/appends {}}
           (job/apply-appends {:a [1 2]
                               :ripmud.job/appends {:a {0 [3 4]}}}
                              [[:a]]))))
  (testing "Multiple Appends"
    (is (= {:a [1 2 3 4 5 6]
            :ripmud.job/appends {}}
           (job/apply-appends {:a [1 2]
                               :ripmud.job/appends {:a {0 [3 4] 1 [5 6]}}}
                              [[:a]]))))
  (testing "Only updates provided keys"
    (is (= {:a [1 2 3 4]
            :b [10 11]
            :ripmud.job/appends {:b {0 [1 2]}}}
           (job/apply-appends {:a [1 2]
                               :b [10 11]
                               :ripmud.job/appends {:a {0 [3 4]} :b {0 [1 2]}}}
                              [[:a]]))))
  (testing "Nested Key"
    (is (= {:a {:b [1 2 3 4]}
            :ripmud.job/appends {:a {}}}
           (job/apply-appends {:a {:b [1 2]}
                               :ripmud.job/appends {:a {:b {0 [3 4]}}}}
                              [[:a :b]]))))
  (testing "Nested Keys, only updates provided keys"
    (is (= {:a {:b [1 2 3 4]
                :c [10 20 30]}}
           (job/apply-appends {:a {:b [1 2] :c [10]}
                               :ripmud.job/appends {:a {:b {0 [3 4]} :c {0 [20 30]}}}})))))

(deftest test-apply-appends-to-whole-map
  (testing "Basic example"
    (is (= {:a [1 2 3 4]}
           (job/apply-appends {:a [1 2]
                               :ripmud.job/appends {:a {0 [3 4]}}}))))
  (testing "Empty appends"
    (is (= {:a [1 2]}
           (job/apply-appends {:a [1 2]}))))
  (testing "Multiple Appends"
    (is (= {:a [1 2 3 4 5 6]}
           (job/apply-appends {:a [1 2]
                               :ripmud.job/appends {:a {0 [3 4] 1 [5 6]}}}))))
  (testing "Missing Key"
    (is (= {:a [3 4]}
           (job/apply-appends {:ripmud.job/appends {:a {0 [3 4]}}}))))
  (testing "Nested Key"
    (is (= {:a {:b [1 2 3 4]}}
           (job/apply-appends {:a {:b [1 2]}
                               :ripmud.job/appends {:a {:b {0 [3 4]}}}}))))
  (testing "Multiple appends, multiple Nested Keys"
    (is (= {:a {:b [1 2 3 4 5 6]
                :c [10 20 30 40 50]}
            :d [100 200]}
           (job/apply-appends {:a {:b [1 2] :c [10]}
                               :ripmud.job/appends {:a {:b {0 [3 4] 1 [5 6]}
                                                        :c {0 [20 30] 1 [40 50]}}
                                                    :d {0 [100 200]}}}))))
  (testing "nil nested map"
    (is (= {:a {:b [1 2]}}
           (job/apply-appends {:a {:b [1 2]}
                               :ripmud.job/appends {:a {:b nil}}}))))
  (testing "Main map key not seqable"
    (is (thrown? Exception (job/apply-appends {:a 1
                                               :ripmud.job/appends {:a {0 [3 4]}}}))))
  (testing "Appends map key not seqable"
    (is (thrown? Exception (job/apply-appends {:a {:b 3}
                                               :ripmud.job/appends {:a [3 4]}}))))
  (testing "Main map key is map"
    (is (thrown? Exception (job/apply-appends {:a {:b 3}
                                               :ripmud.job/appends {:a {0 [3 4]}}})))))


(deftest test-overlapping?
  (testing "Empty jobs don't overlap."
    (is (not (job/overlapping? {:uses #{}
                                :updates #{}}
                               {:uses #{}
                                :updates #{}}))))
  (testing "Jobs using the same things don't overlap."
    (is (not (job/overlapping? {:uses #{[:a] [:b]}
                                :updates #{}}
                               {:uses #{[:a] [:c]}
                                :updates #{}}))))
  (testing "Jobs updating different things don't overlap."
    (is (not (job/overlapping? {:uses #{[:a] [:b]}
                                :updates #{[:b]}}
                               {:uses #{[:a] [:c]}
                                :updates #{[:c]}}))))
  (testing "Job updating something a different job uses overlaps."
    (is (job/overlapping? {:uses #{[:a] [:b]}
                           :updates #{[:a]}}
                          {:uses #{[:a] [:c]}
                           :updates #{}}))
    (is (job/overlapping? {:uses #{[:a] [:b]}
                           :updates #{}}
                          {:uses #{[:a] [:c]}
                           :updates #{[:a]}})))
  (testing "Overlapping works on a prefix basis."
    (is (job/overlapping? {:uses #{[:a] [:b]}
                           :updates #{[:a :b]}}
                          {:uses #{[:a] [:c]}
                           :updates #{}}))
    (is (job/overlapping? {:uses #{[:a] [:b]}
                           :updates #{[:a]}}
                          {:uses #{[:a :b] [:c]}
                           :updates #{}})))
  (testing "Paths with a shared start but diverging end don't overlap."
    (is (not (job/overlapping? {:uses #{[:a :b]}
                                :updates #{[:a :b]}}
                               {:uses #{[:a :c]}
                                :updates #{}}))))
  (testing "Runners updating overlap with eachother."
    (is (job/overlapping? {:runner (TestJobRunner. inc #{[:effects]} #{[:effects]})
                           :uses #{}
                           :updates #{}}
                          {:runner (TestJobRunner. inc #{[:effects]} #{[:effects]})
                           :uses #{}
                           :updates #{}})))
  (testing "Runners updating overlap with jobs."
    (is (job/overlapping? {:runner (TestJobRunner. inc #{[:effects]} #{[:effects]})
                           :uses #{}
                           :updates #{}}
                          {:uses #{[:effects]}
                           :updates #{}}))
    (is (job/overlapping? {:uses #{[:effects]}
                           :updates #{}}
                          {:runner (TestJobRunner. inc #{[:effects]} #{[:effects]})
                           :uses #{}
                           :updates #{}}))
    (is (job/overlapping? {:runner (TestJobRunner. inc #{[:effects]} #{[:effects]})
                           :uses #{}
                           :updates #{}}
                          {:uses #{[:effects :blub]}
                           :updates #{}})))
  (testing "Two jobs that append the same key don't overlap"
    (is (not (job/overlapping? {:uses #{}
                                :updates #{}
                                :appends #{[:effects]}}
                               {:uses #{}
                                :updates #{}
                                :appends #{[:effects]}}))))
  (testing "Appends and uses overlap."
    (is (job/overlapping? {:uses #{[:effects]}
                           :updates #{}
                           :appends #{}}
                          {:uses #{}
                           :updates #{}
                           :appends #{[:effects]}}))))

(deftest test-nodes-in-dependency-graph
  (is (= [:b]
         (job/nodes-in-dependency-graph {:a #{} :b #{:a}} :b #{:a})))
  (is (some #(= % (job/nodes-in-dependency-graph {:a #{} :b #{:a} :c #{:a} :d #{:b :c}} :d #{:a}))
            [[:d :b :c] [:d :c :b]])))

(deftest test-common-roots
  (testing "No common root"
    (is (= #{} (job/common-roots {:a #{} :b #{} :c #{:a}} [:a :b :c]  [:b :c]))))
  (testing "Simple common root"
    (is (= #{:a} (job/common-roots {:a #{} :b #{:a} :c #{:a}} [:a :b :c]  [:b :c]))))
  (testing "Extra branch doesn't ruin the common root"
    (is (= #{:a} (job/common-roots {:a #{} :b #{} :c #{:a} :d #{:a :b}} [:a :b :c :d]  [:c :d]))))
  (testing "Multiple common roots in a line returns the top-most one"
    (is (= #{:f} (job/common-roots {:a #{} :b #{} :c #{:a :b} :d #{:c} :e #{:c} :f #{:d :e} :g #{:f} :h #{:f}}
                               [:a :b :c :d :e :f :g :h]
                               [:f :h]))))
  (testing "Multiple common roots in a criss-cross pattern returns two"
    (is (= #{:j1 :j2} (job/common-roots {:j1 #{} :j2 #{} :j3 #{:j1 :j2} :j4 #{:j1 :j2}}
                                [:j1 :j2 :j3 :j4]
                                [:j3 :j4])))))

(deftest test-jobs->dependency-graph
  (testing "Simple dependency"
    (is (= {:a #{}
            :b #{:a}}
           (job/jobs->dependency-graph [{:id :a
                                         :uses #{[:a]}
                                         :updates #{[:a]}}
                                        {:id :b
                                         :uses #{[:a]}
                                         :updates #{}}]))))
  (testing "Multiple dependencies"
    (is (= {:a #{}
            :b #{}
            :c #{:a :b}}
           (job/jobs->dependency-graph [{:id :a
                                         :uses #{[:a]}
                                         :updates #{[:a]}}
                                        {:id :b
                                         :uses #{[:b]}
                                         :updates #{[:b]}}
                                        {:id :c
                                         :uses #{[:a] [:b]}
                                         :updates #{[:a] [:b]}}]))))
  (testing "Dependencies of dependencies are removed."
    (is (= {:a #{}
            :b #{:a}
            :c #{:b}
            :d #{:c}}
           (job/jobs->dependency-graph [{:id :a
                                         :uses #{[:a]}
                                         :updates #{[:a]}}
                                        {:id :b
                                         :uses #{[:a] [:b]}
                                         :updates #{[:b]}}
                                        {:id :c
                                         :uses #{[:a] [:b] [:c]}
                                         :updates #{[:c]}}
                                        {:id :d
                                         :uses #{[:a] [:c]}
                                         :updates #{}}]))))
  (testing "Complex example"
    (is (= {:a #{}
            :b #{:a}
            :c #{}
            :d #{:c}
            :b+d #{:b :d}
            :e #{}
            :c2 #{:b+d}
            :a+b+e #{:b+d :e}
            :b+c+e #{:a+b+e :c2}}
           (job/jobs->dependency-graph complex-test-jobs)))))

(deftest test-run-job
  (testing "single key"
    (is (= {:a 1 :b 2 :c 4}
           (job/run-job {:a 1 :b 2 :c 3}
                        {:id :test :uses #{[:c]} :updates #{[:c]} :f inc}))))
  (testing "multiple key"
    (is (= {:a 1 :b 3 :c 4}
           (job/run-job {:a 1 :b 2 :c 3}
                        {:id :test :uses #{[:b] [:c]} :updates #{[:b] [:c]}
                         :f (fn [m] (update-vals m inc))}))))
  (testing "updating runner"
    (is (= {:a 10 :b 3 :c 4}
           (job/run-job {:a 1 :b 2 :c 3}
                        {:id :test :uses #{[:b] [:c]} :updates #{[:b] [:c]}
                         :runner (TestJobRunner. #(* 10 %) #{[:a]} #{[:a]})
                         :f (fn [m] (update-vals m inc))}))))
  (testing "multi-key runner update"
    (is (= {:a 10 :b 3 :c 4 :d 40}
           (job/run-job {:a 1 :b 2 :c 3 :d 4}
                        {:id :test :uses #{[:b] [:c]} :updates #{[:b] [:c]}
                         :runner (TestJobRunner. #(update-vals % (fn [v] (* v 10))) #{[:a] [:d]} #{[:a] [:d]})
                         :f (fn [m] (update-vals m inc))})))))

(deftest test-execute-step
  (testing "Shallow pipeline"
    (let [jobs [{:id :j1 :uses #{[:a]} :updates #{[:a]} :f inc}
                {:id :j2 :uses #{[:a]} :updates #{[:a]} :f inc}]
          plan (job/jobs->execution-plan jobs)]
      (is (= {:a 3}
             (job/execute-step plan {:a 1})))))
  (testing "Independend jobs"
    (let [jobs [{:id :j1 :uses #{[:a]} :updates #{[:a]} :f inc}
                {:id :j2 :uses #{[:b]} :updates #{[:b]} :f (partial * 10)}
                {:id :j3 :uses #{[:c]} :updates #{[:c]} :f (partial * 3)}]
          plan (job/jobs->execution-plan jobs)]
      (is (= {:a 2 :b 20 :c 9}
             (job/execute-step plan {:a 1 :b 2 :c 3})))))
  (testing "Deep pipeline"
    (let [jobs [{:id :j1 :uses #{[:a]} :updates #{[:a]} :f inc}
                {:id :j2 :uses #{[:a] [:b]} :updates #{[:a] [:b]} :f #(update-vals % inc)}
                {:id :j3 :uses #{[:c]} :updates #{[:c]} :f inc}
                {:id :j4 :uses #{[:c] [:d]} :updates #{[:c] [:d]} :f #(update-vals % inc)}
                {:id :j5 :uses #{[:c] [:a]} :updates #{[:c] [:a]} :f #(update-vals % inc)}]
          plan (job/jobs->execution-plan jobs)]
      (is (= {:a 4 :b 3 :c 6 :d 5}
             (job/execute-step plan {:a 1 :b 2 :c 3 :d 4})))))
  (testing "Fan-in pipeline"
    (let [jobs [{:id :j1 :uses #{[:a]} :updates #{[:a]} :f inc}
                {:id :j2 :uses #{[:b]} :updates #{[:b]} :f inc}
                {:id :j3 :uses #{[:c]} :updates #{[:c]} :f inc}
                {:id :j4 :uses #{[:a] [:b] [:c]} :updates #{[:a] [:b] [:c]} :f #(update-vals % (partial * 10))}]
          plan (job/jobs->execution-plan jobs)]
      (is (= {:a 20 :b 30 :c 40}
             (job/execute-step plan {:a 1 :b 2 :c 3})))))
  (testing "Common root"
    (let [jobs [{:id :j1 :uses #{[:a]} :updates #{[:a]} :f inc}
                {:id :j2 :uses #{[:b]} :updates #{[:b]} :f inc}
                {:id :j3 :uses #{[:a] [:b]} :updates #{[:a] [:b]} :f #(update-vals % (partial * 10))}
                {:id :j4 :uses #{[:a]} :updates #{[:a]} :f inc}
                {:id :j5 :uses #{[:b]} :updates #{[:b]} :f inc}]
          plan (job/jobs->execution-plan jobs)]
      (is (= {:a 21 :b 31 :c 3}
             (job/execute-step plan {:a 1 :b 2 :c 3})))))
  (testing "Multiple common roots"
    (let [jobs [{:id :j1 :uses #{[:a] [:c]} :updates #{[:a] [:c]} :f #(update-vals % (partial * 3))}
                {:id :j2 :uses #{[:b] [:d]} :updates #{[:b] [:d]} :f #(update-vals % (partial * 5))}
                {:id :j3 :uses #{[:c] [:d]} :updates #{[:c] [:d]} :f #(update-vals % (partial * 7))}
                {:id :j4 :uses #{[:a] [:b]} :updates #{[:a] [:b]} :f #(update-vals % (partial * 11))}]
          plan (job/jobs->execution-plan jobs)]
      (is (= {:a 33 :b 110 :c 63 :d 140}
             (job/execute-step plan {:a 1 :b 2 :c 3 :d 4}))))
    (let [jobs [{:id :j1 :uses #{[:a] [:c]} :updates #{[:a] [:c]} :f #(update-vals % (partial * 3))}
                {:id :j2 :uses #{[:b] [:d]} :updates #{[:b] [:d]} :f #(update-vals % (partial * 5))}
                {:id :j3 :uses #{[:c] [:d]} :updates #{[:c] [:d]} :f #(update-vals % (partial * 7))}
                {:id :j4 :uses #{[:a] [:b]} :updates #{[:a] [:b]} :f #(update-vals % (partial * 11))}
                {:id :j5 :uses #{[:c] [:e]} :updates #{[:c] [:e]} :f #(update-vals % inc)}
                {:id :j6 :uses #{[:c]} :updates #{[:c]} :f inc}]
          plan (job/jobs->execution-plan jobs)]
      (is (= {:a 33 :b 110 :c 65 :d 140 :e 6}
             (job/execute-step plan {:a 1 :b 2 :c 3 :d 4 :e 5}))))))

(deftest test-execute-step-appends
  (testing "Independend jobs"
    (let [jobs [{:id :j1 :uses #{[:a]} :updates #{[:a]} :appends #{[:d]} :f (fn [v] {:a (inc v) :d [1]})}
                {:id :j2 :uses #{[:b]} :updates #{[:b]} :appends #{[:d]} :f (fn [v] {:b (* 10 v) :d [10]})}
                {:id :j3 :uses #{[:c]} :updates #{[:c]} :appends #{[:d]} :f (fn [v] {:c (* 3 v) :d [3]})}]
          plan (job/jobs->execution-plan jobs)]
      (is (= {:a 2 :b 20 :c 9 :d [1 10 3]}
             (job/execute-step plan {:a 1 :b 2 :c 3}))))
  (testing "Appends are applied when job uses them"
    (let [jobs [{:id :j1 :uses #{[:a]} :updates #{[:a]} :appends #{[:d]} :f (fn [v] {:a (inc v) :d [1]})}
                {:id :j2 :uses #{[:b]} :updates #{[:b]} :appends #{[:d]} :f (fn [v] {:b (* 10 v) :d [10]})}
                {:id :j3 :uses #{[:c]} :updates #{[:c]} :appends #{[:d]} :f (fn [v] {:c (* 3 v) :d [3]})}
                {:id :j4 :uses #{[:d]} :updates #{[:d]} :f (fn [v] (mapv inc v))}]
          plan (job/jobs->execution-plan jobs)]
      (is (= {:a 2 :b 20 :c 9 :d [2 11 4]}
             (job/execute-step plan {:a 1 :b 2 :c 3})))))
   (testing "Multiple common roots"
    (let [jobs [{:id :j1 :uses #{[:a] [:c]} :updates #{[:a] [:c]} :appends #{[:e]} :f #(-> % (update-vals (partial * 3)) (assoc :e [3]))}
                {:id :j2 :uses #{[:b] [:d]} :updates #{[:b] [:d]} :appends #{[:e]} :f #(-> % (update-vals (partial * 5)) (assoc :e [5]))}
                {:id :j3 :uses #{[:c] [:d]} :updates #{[:c] [:d]} :appends #{[:e]} :f #(-> % (update-vals (partial * 7)) (assoc :e [7]))}
                {:id :j4 :uses #{[:a] [:b]} :updates #{[:a] [:b]} :appends #{[:e]} :f #(-> % (update-vals (partial * 11)) (assoc :e [11]))}]
          plan (job/jobs->execution-plan jobs)]
      (is (= {:a 33 :b 110 :c 63 :d 140 :e [3 5 7 11]}
             (job/execute-step plan {:a 1 :b 2 :c 3 :d 4})))))))
