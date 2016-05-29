(ns views.basic-system-init-tests
  (:use
    clojure.test
    views.test-helpers
    views.protocols
    views.core
    views.test-view-system)
  (:import
    (views.test_view_system MemoryView)
    (clojure.lang Atom)))

(use-fixtures :each reset-test-views-system)


(defn dummy-send-fn [subscriber-key [view-sig view-data]])

(def test-options (merge default-options
                         {:views views
                          :send-fn dummy-send-fn}))


;; tests

(deftest inits-with-correct-config-and-shutsdown-correctly
  (let [options test-options
        ; 1. init views
        init-returned-atom (init! test-views-system test-options)]
    (is (instance? Atom init-returned-atom))
    (is (= init-returned-atom test-views-system))
    (is (seq @test-views-system))
    (is (= dummy-send-fn (:send-fn @test-views-system)))
    (is (and (contains-view? test-views-system :foo)
             (contains-view? test-views-system :bar)
             (contains-view? test-views-system :baz)))
    (is (not (:logging? @test-views-system)))
    (is (not (collecting-stats? test-views-system)))
    (is (empty? (subscribed-views test-views-system)))
    (let [refresh-watcher (:refresh-watcher @test-views-system)
          workers         (:workers @test-views-system)]
      (is (.isAlive ^Thread refresh-watcher))
      (is (= (:worker-threads options) (count workers)))
      (doseq [^Thread t workers]
        (is (.isAlive t)))
      ; 2. shutdown views (and wait for all threads to also finish)
      (shutdown! test-views-system)
      (is (empty? @test-views-system))
      (is (not (.isAlive ^Thread refresh-watcher)))
      (doseq [^Thread t workers]
        (is (not (.isAlive t)))))))

(deftest init-without-existing-view-system-atom
  (let [options test-options]
    (let [init-created-atom (init! options)]
      (is (instance? Atom init-created-atom))
      (is (seq @init-created-atom))
      (is (= dummy-send-fn (:send-fn @init-created-atom)))
      (is (and (contains-view? init-created-atom :foo)
               (contains-view? init-created-atom :bar)
               (contains-view? init-created-atom :baz)))
      (shutdown! init-created-atom)
      (is (empty? @init-created-atom)))))

(deftest init-can-also-start-logger
  (let [options (-> test-options
                    (assoc :stats-log-interval 10000))]
    ; 1. init views
    (init! test-views-system options)
    (is (seq (:statistics @test-views-system)))
    (is (:logging? @test-views-system))
    (is (collecting-stats? test-views-system))
    (let [logger-thread (get-in @test-views-system [:statistics :logger])]
      (is (.isAlive ^Thread logger-thread))
      ; 2. shutdown views
      (shutdown! test-views-system)
      (is (nil? (get-in @test-views-system [:statistics :logger])))
      (is (not (.isAlive ^Thread logger-thread))))))

(deftest can-add-new-views-after-init
  (let [options test-options]
    ; 1. init views
    (init! test-views-system options)
    (is (and (contains-view? test-views-system :foo)
             (contains-view? test-views-system :bar)
             (contains-view? test-views-system :baz)))
    ; 2. add new views
    (add-views! test-views-system
                [(MemoryView. :one [:one])
                 (MemoryView. :two [:two])])
    (is (and (contains-view? test-views-system :foo)
             (contains-view? test-views-system :bar)
             (contains-view? test-views-system :baz)
             (contains-view? test-views-system :one)
             (contains-view? test-views-system :two)))
    ; 3. shutdown views
    (shutdown! test-views-system)))

(deftest can-replace-views-after-init
  (let [options          test-options
        replacement-view (MemoryView. :foo [:new-foo])]
    ; 1. init views
    (init! test-views-system options)
    (is (and (contains-view? test-views-system :foo)
             (contains-view? test-views-system :bar)
             (contains-view? test-views-system :baz)))
    (is (not= replacement-view (get-in @test-views-system [:views :foo])))
    ; 2. add view. has same id so should replace existing one
    (add-views! test-views-system [replacement-view])
    (is (and (contains-view? test-views-system :foo)
             (contains-view? test-views-system :bar)
             (contains-view? test-views-system :baz)))
    (is (= replacement-view (get-in @test-views-system [:views :foo])))
    ; 3. shutdown views
    (shutdown! test-views-system)))
