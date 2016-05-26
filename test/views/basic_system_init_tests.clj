(ns views.basic-system-init-tests
  (:use
    clojure.test
    views.protocols
    views.core))

(defn reset-state-fixture! [f]
  (reset! view-system {})
  (reset! statistics {})
  (f))

(use-fixtures :each reset-state-fixture!)



(def memory-database (atom {}))

(defrecord MemoryView [id ks]
  IView
  (id [_] id)
  (data [_ namespace parameters]
    (get-in @memory-database (-> [namespace]
                                 (into ks)
                                 (into parameters))))
  (relevant? [_ namespace parameters hints]
    (some #(and (= namespace (:namespace %))
                (= ks (:hint %)))
          hints)))

(def views
  [(MemoryView. :foo [:foo])
   (MemoryView. :bar [:bar])
   (MemoryView. :baz [:baz])])

(defn dummy-send-fn [subscriber-key [view-sig view-data]])



;; test helper functions

(defn contains-view?
  [view-id]
  (let [view (get (:views @view-system) view-id)]
    (and view
         (satisfies? IView view))))



;; tests

(deftest inits-with-correct-config-and-shutsdown-correctly
  (let [options default-options]
    (is (empty? @view-system))
    ; 1. init views
    (init! views dummy-send-fn options)
    (is (seq @view-system))
    (is (= dummy-send-fn (:send-fn @view-system)))
    (is (and (contains-view? :foo)
             (contains-view? :bar)
             (contains-view? :baz)))
    (is (not (:logging? @view-system)))
    (is (not (collect-stats?)))
    (is (empty? (subscribed-views)))
    (let [refresh-watcher (:refresh-watcher @view-system)
          workers         (:workers @view-system)]
      (is (.isAlive ^Thread refresh-watcher))
      (is (= (:worker-threads options) (count workers)))
      (doseq [^Thread t workers]
        (is (.isAlive t)))
      ; 2. shutdown views (and wait for all threads to also finish)
      (shutdown! true)
      (is (empty? @view-system))
      (is (empty? @statistics))
      (is (not (.isAlive ^Thread refresh-watcher)))
      (doseq [^Thread t workers]
        (is (not (.isAlive t)))))))

(deftest init-can-also-start-logger
  (let [options (-> default-options
                    (assoc :stats-log-interval 10000))]
    ; 1. init views
    (init! views dummy-send-fn options)
    (is (seq @statistics))
    (is (:logging? @view-system))
    (is (collect-stats?))
    (let [logger-thread (:logger @statistics)]
      (is (.isAlive ^Thread logger-thread))
      ; 2. shutdown views
      (shutdown! true)
      (is (nil? (:logger @statistics)))
      (is (not (.isAlive ^Thread logger-thread))))))

(deftest can-add-new-views-after-init
  (let [options default-options]
    ; 1. init views
    (init! views dummy-send-fn options)
    (is (and (contains-view? :foo)
             (contains-view? :bar)
             (contains-view? :baz)))
    ; 2. add new views
    (add-views! [(MemoryView. :one [:one])
                 (MemoryView. :two [:two])])
    (is (and (contains-view? :foo)
             (contains-view? :bar)
             (contains-view? :baz)
             (contains-view? :one)
             (contains-view? :two)))
    ; 3. shutdown views
    (shutdown! true)))

(deftest can-replace-views-after-init
  (let [options          default-options
        replacement-view (MemoryView. :foo [:new-foo])]
    ; 1. init views
    (init! views dummy-send-fn options)
    (is (and (contains-view? :foo)
             (contains-view? :bar)
             (contains-view? :baz)))
    (is (not= replacement-view (get-in @view-system [:views :foo])))
    ; 2. add view. has same id so should replace existing one
    (add-views! [replacement-view])
    (is (and (contains-view? :foo)
             (contains-view? :bar)
             (contains-view? :baz)))
    (is (= replacement-view (get-in @view-system [:views :foo])))
    ; 3. shutdown views
    (shutdown! true)))
