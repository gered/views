(ns views.basic-system-init-tests
  (:use
    clojure.test
    views.test-helpers
    views.protocols
    views.core
    views.test-memory-db)
  (:import (views.test_memory_db MemoryView)))

(defn reset-state-fixture! [f]
  (reset! view-system {})
  (f))

(use-fixtures :each reset-state-fixture!)


(defn dummy-send-fn [subscriber-key [view-sig view-data]])



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
      (is (not (.isAlive ^Thread refresh-watcher)))
      (doseq [^Thread t workers]
        (is (not (.isAlive t)))))))

(deftest init-can-also-start-logger
  (let [options (-> default-options
                    (assoc :stats-log-interval 10000))]
    ; 1. init views
    (init! views dummy-send-fn options)
    (is (seq (:statistics @view-system)))
    (is (:logging? @view-system))
    (is (collect-stats?))
    (let [logger-thread (get-in @view-system [:statistics :logger])]
      (is (.isAlive ^Thread logger-thread))
      ; 2. shutdown views
      (shutdown! true)
      (is (nil? (get-in @view-system [:statistics :logger])))
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
