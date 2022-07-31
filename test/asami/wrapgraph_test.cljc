(ns asami.wrapgraph-test
  (:require [asami.graph :refer [graph-add graph-transact resolve-pattern count-pattern]]
     [asami.index :refer [empty-graph]]
     [asami.wrapgraph :refer [wrap-graph unwrap-graph]]
     [schema.test :as st :refer [deftest] :refer-macros [deftest]]
     [clojure.test :as t :refer [testing is run-tests] :refer-macros [testing is run-tests]]))

(t/use-fixtures :once st/validate-schemas)

(def data
  [[:a :p1 :x]
   [:a :p1 :y]
   [:a :p2 :z]
   [:a :p3 :x]
   [:b :p1 :x]
   [:b :p2 :x]
   [:b :p3 :z]
   [:c :p4 :t]])

(defn assert-data
  ([g d]
   (assert-data g d 1))
  ([g d tx]
   (reduce (fn [g [s p o]] (graph-add g s p o tx)) g d)))

(defn unordered-resolve
  [g pattern]
  (into #{} (resolve-pattern g pattern)))

(defn do-load-test
  [g]
  (let [r1 (unordered-resolve g '[:a ?a ?b])
        r2 (unordered-resolve g '[?a :p2 ?b])
        r3 (unordered-resolve g '[:a :p1 ?a])
        r4 (unordered-resolve g '[?a :p2 :z])
        r5 (unordered-resolve g '[:a ?a :x])
        r6 (unordered-resolve g '[:a :p4 ?a])
        r6' (unordered-resolve g '[:a :p3 ?a])
        r7 (unordered-resolve g '[:a :p1 :x])
        r8 (unordered-resolve g '[:a :p1 :b])
        r9 (unordered-resolve g '[?a ?b ?c])]
    (is (= #{[:p1 :x]
             [:p1 :y]
             [:p2 :z]
             [:p3 :x]} r1))
    (is (= #{[:a :z]
             [:b :x]} r2))
    (is (= #{[:x]
             [:y]} r3))
    (is (= #{[:a]} r4))
    (is (= #{[:p1]
             [:p3]} r5))
    (is (empty? r6))
    (is (= #{[:x]} r6'))
    (is (= #{[]} r7))
    (is (empty? r8))
    (is (= (into #{} data) r9))))

(defn wrap-graph-tx [data] (wrap-graph data 1))

(deftest test-wrap-empty
  (do-load-test (assert-data (wrap-graph empty-graph 0) data)))

(deftest test-wrap-full
  (do-load-test (wrap-graph-tx (assert-data empty-graph data))))

(deftest test-wrap-insert
  (->> (take 4 data)
       (assert-data empty-graph)
       wrap-graph-tx
       (#(assert-data % (drop 4 data)))
       do-load-test))

(deftest test-wrap-2insert
  (->> (take 4 data)
       (assert-data empty-graph)
       wrap-graph-tx
       (#(assert-data % (take 2 (drop 4 data))))
       (#(assert-data % (drop 6 data)))
       do-load-test))

(defn do-test-count [g]
  (let [r1 (count-pattern g '[:a ?a ?b])
        r2 (count-pattern g '[?a :p2 ?b])
        r3 (count-pattern g '[:a :p1 ?a])
        r4 (count-pattern g '[?a :p2 :z])
        r5 (count-pattern g '[:a ?a :x])
        r6 (count-pattern g '[:a :p4 ?a])
        r7 (count-pattern g '[:a :p1 :x])
        r8 (count-pattern g '[:a :p1 :b])
        r9 (count-pattern g '[?a ?b ?c])]
    (is (= 4 r1))
    (is (= 2 r2))
    (is (= 2 r3))
    (is (= 1 r4))
    (is (= 2 r5))
    (is (zero? r6))
    (is (= 1 r7))
    (is (zero? r8))
    (is (= 8 r9))))

(deftest test-count-wrap-empty
  (do-test-count (assert-data (wrap-graph empty-graph 0) data)))

(deftest test-count-wrap-full
  (do-test-count (wrap-graph-tx (assert-data empty-graph data))))

(deftest test-count-wrap-insert
  (->> (take 4 data)
       (assert-data empty-graph)
       wrap-graph-tx
       (#(assert-data % (drop 4 data)))
       do-test-count))

(deftest test-count-wrap-2insert
  (->> (take 4 data)
       (assert-data empty-graph)
       wrap-graph-tx
       (#(assert-data % (take 2 (drop 4 data))))
       (#(assert-data % (drop 6 data)))
       do-test-count))

(deftest test-delete
  (let [g' (wrap-graph (assert-data empty-graph data) 0)
        g (graph-transact g' 2 [] [[:a :p2 :z] [:c :p4 :t]])
        r1 (unordered-resolve g '[:a ?a ?b])
        r2 (unordered-resolve g '[?a :p2 ?b])
        r3 (unordered-resolve g '[:a :p1 ?a])
        r4 (unordered-resolve g '[?a :p2 :z])
        r5 (unordered-resolve g '[:a ?a :x])
        r6 (unordered-resolve g '[:a :p4 ?a])
        r6' (unordered-resolve g '[:a :p3 ?a])
        r7 (unordered-resolve g '[:a :p1 :x])
        r8 (unordered-resolve g '[:a :p1 :b])
        r9 (unordered-resolve g '[?a ?b ?c])]
    (is (= #{[:p1 :x]
             [:p1 :y]
             [:p3 :x]} r1))
    (is (= #{[:b :x]} r2))
    (is (= #{[:x]
             [:y]} r3))
    (is (empty? r4))
    (is (= #{[:p1]
             [:p3]} r5))
    (is (empty? r6))
    (is (= #{[:x]} r6'))
    (is (= #{[]} r7))
    (is (empty? r8))
    (is (= (into #{} (remove #{[:a :p2 :z] [:c :p4 :t]} data)) r9))))

(deftest test-change
  (let [g' (wrap-graph-tx (assert-data empty-graph data))
        g (graph-transact g' 2 [[:a :p3 :z] [:c :p2 :u]] [[:a :p2 :z] [:c :p4 :t]])
        r1 (unordered-resolve g '[:a ?a ?b])
        r2 (unordered-resolve g '[?a :p2 ?b])
        r3 (unordered-resolve g '[:a :p1 ?a])
        r4 (unordered-resolve g '[?a :p2 :z])
        r5 (unordered-resolve g '[:a ?a :x])
        r6 (unordered-resolve g '[:a :p4 ?a])
        r6' (unordered-resolve g '[:a :p3 ?a])
        r7 (unordered-resolve g '[:a :p1 :x])
        r8 (unordered-resolve g '[:a :p1 :b])
        r9 (unordered-resolve g '[?a ?b ?c])]
    (is (= #{[:p1 :x]
             [:p1 :y]
             [:p3 :z]
             [:p3 :x]} r1))
    (is (= #{[:b :x] [:c :u]} r2))
    (is (= #{[:x]
             [:y]} r3))
    (is (empty? r4))
    (is (= #{[:p1]
             [:p3]} r5))
    (is (empty? r6))
    (is (= #{[:x] [:z]} r6'))
    (is (= #{[]} r7))
    (is (empty? r8))
    (is (= (->> data
                (remove #{[:a :p2 :z] [:c :p4 :t]})
                (concat [[:a :p3 :z] [:c :p2 :u]])
                (into #{}))
           r9))))

(deftest test-change-unwrapped
  (let [g' (wrap-graph-tx (assert-data empty-graph data))
        g (graph-transact g' 2 [[:a :p3 :z] [:c :p2 :u]] [[:a :p2 :z] [:c :p4 :t]])
        gu (unwrap-graph g 2)
        r (unordered-resolve g '[?a ?b ?c])
        ru (unordered-resolve gu '[?a ?b ?c])
        expected (->> data
                      (remove #{[:a :p2 :z] [:c :p4 :t]})
                      (concat [[:a :p3 :z] [:c :p2 :u]])
                      (into #{}))]
    (is (= expected r))
    (is (= expected ru))))

#?(:cljs (run-tests))

