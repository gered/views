(ns views.hint-tests
  (:use
    clojure.test
    views.test-helpers
    views.protocols
    views.core
    views.test-memory-db))


(def test-sent-data
  (atom []))

(defn test-send-fn [subscriber-key [view-sig view-data]]
  (swap! test-sent-data conj {:subscriber-key subscriber-key
                              :view-sig       view-sig
                              :view-data      view-data}))


; fixtures

(defn reset-system-fixture [f]
  (reset! view-system {})
  (f)
  (shutdown! true))

(defn clear-sent-data-fixture [f]
  (reset! test-sent-data [])
  (f))

(use-fixtures :each clear-sent-data-fixture reset-system-fixture reset-memory-db-fixture)



;; tests

(deftest refresh-views!-instantly-attempts-view-refresh-with-given-hints
  (let [options         default-options
        hints-refreshed (atom [])]
    ; 1. init views
    (init! views test-send-fn options)
    ; with a view subscription (any subscription will do)
    (with-redefs [subscribed-views (fn [] #{(->view-sig :namespace :fake-view [])})
                  refresh-view!    (fn [hints _] (swap! hints-refreshed into hints))]
      ; 2. trigger refresh by calling refresh-views! with hints
      (refresh-views! [(hint :namespace [:foo] :fake-type)])
      (is (contains-only? @hints-refreshed
                          [(hint :namespace [:foo] :fake-type)]))
      (reset! hints-refreshed [])
      ; 3. same thing again, but passing in multiple hints
      (refresh-views! [(hint :namespace [:foo] :fake-type)
                       (hint :namespace [:bar] :fake-type)])
      (is (contains-only? @hints-refreshed
                          [(hint :namespace [:foo] :fake-type)
                           (hint :namespace [:bar] :fake-type)]))
      (reset! hints-refreshed []))
    ; now, without any view subscriptions
    (with-redefs [subscribed-views (fn [] #{})
                  refresh-view!    (fn [hints _] (swap! hints-refreshed into hints))]
      ; 4. again trigger refresh by calling refresh-views! with hints
      (refresh-views! [(hint :namespace [:foo] :fake-type)])
      (is (empty? @hints-refreshed))
      (reset! hints-refreshed []))))

(deftest refresh-watcher-runs-at-specified-interval-and-picks-up-queued-hints
  (let [options         default-options
        hints-refreshed (atom [])]
    (with-redefs [subscribed-views (fn [] #{(->view-sig :namespace :fake-view [])})
                  refresh-view!    (fn [hints _] (swap! hints-refreshed into hints))]
      ; 1. init views
      (init! views test-send-fn options)
      ; 2. queue a hint and wait until the next refresh interval
      (queue-hints! [(hint :namespace [:foo] :fake-type)])
      (wait-for-refresh-interval options)
      (is (contains-only? @hints-refreshed
                          [(hint :namespace [:foo] :fake-type)]))
      (reset! hints-refreshed [])
      ; 3. queue multiple hints and wait again
      (queue-hints! [(hint :namespace [:foo] :fake-type)
                     (hint :namespace [:bar] :fake-type)])
      (wait-for-refresh-interval options)
      (is (contains-only? @hints-refreshed
                          [(hint :namespace [:foo] :fake-type)
                           (hint :namespace [:bar] :fake-type)]))
      (reset! hints-refreshed [])
      ; 4. queue up no hints and wait
      (wait-for-refresh-interval options)
      (reset! hints-refreshed []))))

(deftest refresh-worker-thread-processes-relevant-hints
  (let [options         default-options
        views-refreshed (atom [])]
    ; 1. init views
    (init! views test-send-fn options)
    (with-redefs [subscribed-views (fn [] #{(->view-sig :a :foo [])})
                  do-view-refresh! (fn [view-sig] (swap! views-refreshed into [view-sig]))]
      ; 2. trigger refresh by calling refresh-views! with relevant hint
      (refresh-views! [(hint :a [:foo] memory-view-hint-type)])
      (wait-for-refresh-views)
      (is (contains-only? @views-refreshed [(->view-sig :a :foo [])]))
      (reset! views-refreshed [])
      ; 3. same thing again, but passing in multiple hints (1 relevant, 1 not)
      (refresh-views! [(hint :a [:foo] memory-view-hint-type)
                       (hint :a [:bar] memory-view-hint-type)])
      (wait-for-refresh-views)
      (is (contains-only? @views-refreshed [(->view-sig :a :foo [])]))
      (reset! views-refreshed [])
      ; 4. and lastly, passing in only irrelevant hints
      (refresh-views! [(hint :b [:foo] memory-view-hint-type)
                       (hint :a [:foo] :some-other-type)])
      (wait-for-refresh-views)
      (is (empty? @views-refreshed))
      (reset! views-refreshed []))))

; this test is really just testing that our helper function memory-db-assoc-in! works as we expect it to
; (otherwise, it is entirely redundant given the above tests)
(deftest test-memory-db-operation-triggers-proper-refresh-hints
  (let [options         default-options
        hints-refreshed (atom [])
        views-refreshed (atom [])]
    ; 1. init views
    (init! views test-send-fn options)
    ; first tests verifying that correct hints are being sent out (don't care if relevant or not yet)
    (with-redefs [subscribed-views (fn [] #{(->view-sig :a :foo [])})
                  refresh-view!    (fn [hints _] (swap! hints-refreshed into hints))]
      (memory-db-assoc-in! :a [:foo] 42)
      (memory-db-assoc-in! :a [:bar] 3.14)
      (memory-db-assoc-in! :b [:baz] [10 20 30])
      (wait-for-refresh-views)
      (is (contains-only? @hints-refreshed
                          [(hint :a [:foo] memory-view-hint-type)
                           (hint :a [:bar] memory-view-hint-type)
                           (hint :b [:baz] memory-view-hint-type)]))
      (reset! views-refreshed []))
    ; now we test that relevant views were recognized as relevant and forwarded on to be used to
    ; trigger actual refreshes of view data
    (with-redefs [subscribed-views (fn [] #{(->view-sig :a :foo [])})
                  do-view-refresh! (fn [view-sig] (swap! views-refreshed into [view-sig]))]
      ; 2. update memory database (in a location covered by the subscribed view)
      (memory-db-assoc-in! :a [:foo] 1337)
      (wait-for-refresh-interval options)
      (is (contains-only? @views-refreshed [(->view-sig :a :foo [])]))
      (reset! views-refreshed [])
      ; 3. same thing again, but update a different location not covered by any subscription
      (memory-db-assoc-in! :a [:bar] 1234.5678)
      (wait-for-refresh-interval options)
      (is (empty? @views-refreshed)))))

(deftest relevant-hints-cause-refreshed-data-to-be-sent-to-subscriber
  (let [options        default-options
        subscriber-key 123
        view-sig       (->view-sig :a :foo [])]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. subscribe to a view
    (let [original-view-data (get-view-data view-sig)
          updated-view-data  21
          subscribe-result   (subscribe! view-sig subscriber-key nil)]
      ; 3. block until subscription finishes. we don't care about the initial view data refresh
      (while (not (realized? subscribe-result)))
      (reset! test-sent-data [])
      (is (= (hash original-view-data) (get-in @view-system [:hashes view-sig])))
      ; 4. change some test data that is covered by the view subscription
      (memory-db-assoc-in! :a [:foo] updated-view-data)
      (wait-for-refresh-views)
      (is (= (hash updated-view-data) (get-in @view-system [:hashes view-sig])))
      (is (contains-only? @test-sent-data
                          [{:subscriber-key subscriber-key
                            :view-sig       (dissoc view-sig :namespace)
                            :view-data      updated-view-data}])))))

(deftest irrelevant-hints-dont-trigger-refreshes
  (let [options        default-options
        subscriber-key 123
        view-sig       (->view-sig :a :foo [])]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. subscribe to a view
    (let [subscribe-result (subscribe! view-sig subscriber-key nil)]
      ; 3. block until subscription finishes. we don't care about the initial view data refresh
      (while (not (realized? subscribe-result)))
      (reset! test-sent-data [])
      ; 4. change some test data that is NOT covered by the view subscription
      (memory-db-assoc-in! :b [:foo] 6)
      (memory-db-assoc-in! :a [:bar] 7)
      (wait-for-refresh-views)
      (is (empty? @test-sent-data)))))

(deftest refreshes-not-sent-if-view-data-is-unchanged-since-last-refresh
  (let [options        default-options
        subscriber-key 123
        view-sig       (->view-sig :a :foo [])]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. subscribe to a view
    (let [updated-view-data 1111
          subscribe-result  (subscribe! view-sig subscriber-key nil)]
      ; 3. block until subscription finishes. we don't care about the initial view data refresh
      (while (not (realized? subscribe-result)))
      (reset! test-sent-data [])
      ; 4. change some test data, will cause a refresh to be sent out
      (memory-db-assoc-in! :a [:foo] updated-view-data)
      (wait-for-refresh-views)
      (is (= (hash updated-view-data) (get-in @view-system [:hashes view-sig])))
      (is (contains-only? @test-sent-data
                          [{:subscriber-key subscriber-key
                            :view-sig       (dissoc view-sig :namespace)
                            :view-data      updated-view-data}]))
      (reset! test-sent-data [])
      ; 5. manually trigger another refresh for the view
      (refresh-views! [(hint :a [:foo] memory-view-hint-type)])
      (wait-for-refresh-views)
      (is (empty? @test-sent-data))
      ; 6. also try "updating" the db with the same values
      (memory-db-assoc-in! :a [:foo] updated-view-data)
      (wait-for-refresh-views)
      (is (empty? @test-sent-data)))))

(deftest refresh-queue-drops-duplicate-hints
  (let [options        (-> default-options
                           ; enable statistics collection
                           (assoc :stats-log-interval 10000))
        subscriber-key 123
        view-sig       (->view-sig :a :foo [])]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. prematurely stop refresh worker threads so that we can more easily inspect the
    ;    internal refresh queue's entries. the refresh worker threads are what remove
    ;    hints from the refresh queue as they are added to it.
    (stop-refresh-worker-threads)
    ; 3. subscribe to a view
    (let [subscribe-result (subscribe! view-sig subscriber-key nil)]
      ; 4. block until subscription finishes
      (while (not (realized? subscribe-result)))
      (is (= 0 (get-in @view-system [:statistics :deduplicated])))
      ; 5. add duplicate hints by changing the same set of data twice
      ;    (hints will stay in the queue forever because we stopped the worker threads)
      (memory-db-assoc-in! :a [:foo] 6)
      (memory-db-assoc-in! :a [:foo] 7)
      (wait-for-refresh-views)
      (is (= 1 (get-in @view-system [:statistics :deduplicated])))
      (is (= [view-sig]
             (vec (:refresh-queue @view-system)))))))

(deftest refresh-queue-drops-hints-when-full
  (let [options        (-> default-options
                           ; enable statistics collection
                           (assoc :stats-log-interval 10000
                                  :refresh-queue-size 1))
        subscriber-key 123
        view-sig-a     (->view-sig :a :foo [])
        view-sig-b     (->view-sig :b :foo [])]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. prematurely stop refresh worker threads so that we can more easily inspect the
    ;    internal refresh queue's entries. the refresh worker threads are what remove
    ;    hints from the refresh queue as they are added to it.
    (stop-refresh-worker-threads)
    ; 3. subscribe to a view
    ; note: log* redef is to suppress error log output which will normally happen whenever
    ;       another item is added to the refresh queue when it's already full
    (with-redefs [clojure.tools.logging/log* (fn [& _])]
      (let [subscribe-a (subscribe! view-sig-a subscriber-key nil)
            subscribe-b (subscribe! view-sig-b subscriber-key nil)]
        ; 4. block until subscription finishes
        (while (or (not (realized? subscribe-a))
                   (not (realized? subscribe-b))))
        (is (= 0 (get-in @view-system [:statistics :dropped])))
        ; 5. change some data affecting the subscribed view, resulting in more then 1 hint
        ;    being added to the refresh queue
        (memory-db-assoc-in! :a [:foo] 101010)
        (memory-db-assoc-in! :b [:foo] 010101)
        (wait-for-refresh-views)
        (is (= 1 (get-in @view-system [:statistics :dropped])))
        (is (= [view-sig-a]
               (vec (:refresh-queue @view-system))))))))
