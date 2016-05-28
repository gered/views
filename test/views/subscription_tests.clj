(ns views.subscription-tests
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

(deftest can-subscribe-to-a-view
  (let [options        default-options
        subscriber-key 123
        view-sig       (->view-sig :namespace :foo [])
        context        {:my-data "arbitrary application context data"}]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. subscribe to a view
    (let [subscribe-result (subscribe! view-sig subscriber-key context)]
      (is (future? subscribe-result))
      (is (= [subscriber-key] (keys (:subscribed @view-system))))
      (is (= #{view-sig} (get-in @view-system [:subscribed subscriber-key])))
      (is (= #{subscriber-key} (get-in @view-system [:subscribers view-sig])))
      ; 3. block until subscription finishes (data retrieval + initial view refresh)
      ;    (in this particular unit test, there is really no point in waiting)
      (while (not (realized? subscribe-result)))
      (is (= #{view-sig} (subscribed-views))))))

(deftest subscribing-results-in-initial-view-data-being-sent
  (let [options        default-options
        subscriber-key 123
        view-sig       (->view-sig :a :foo [])
        context        {:my-data "arbitrary application context data"}]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. subscribe to a view
    (let [view-data        (get-view-data view-sig)
          subscribe-result (subscribe! view-sig subscriber-key context)]
      ; 3. block until subscription finishes (data retrieval + initial view refresh)
      (while (not (realized? subscribe-result)))
      (is (= #{view-sig} (subscribed-views)))
      (is (= (hash view-data) (get-in @view-system [:hashes view-sig])))
      (is (contains-only? @test-sent-data
                          [{:subscriber-key subscriber-key
                            :view-sig       (dissoc view-sig :namespace)
                            :view-data      view-data}])))))

(deftest can-unsubscribe-from-a-view
  (let [options        default-options
        subscriber-key 123
        view-sig       (->view-sig :a :foo [])
        context        {:my-data "arbitrary application context data"}]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. subscribe to a view
    (let [view-data        (get-view-data view-sig)
          subscribe-result (subscribe! view-sig subscriber-key context)]
      (is (= [subscriber-key] (keys (:subscribed @view-system))))
      (is (= #{view-sig} (get-in @view-system [:subscribed subscriber-key])))
      (is (= #{subscriber-key} (get-in @view-system [:subscribers view-sig])))
      (is (= #{view-sig} (subscribed-views)))
      ; 3. block until subscription finishes
      (while (not (realized? subscribe-result)))
      (is (= (hash view-data) (get-in @view-system [:hashes view-sig])))
      ; 4. unsubscribe
      (unsubscribe! view-sig subscriber-key context)
      (is (empty? (keys (:subscribed @view-system))))
      (is (empty? (keys (:subscribers @view-system))))
      (is (empty? (subscribed-views)))
      (is (empty? (:hashes @view-system))))))

(deftest multiple-subscription-and-unsubscriptions
  (let [options          default-options
        subscriber-key-a 123
        subscriber-key-b 456
        view-sig         (->view-sig :a :foo [])]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. subscribe to a view
    (let [view-data        (get-view-data view-sig)
          subscribe-a      (subscribe! view-sig subscriber-key-a nil)
          subscribe-b      (subscribe! view-sig subscriber-key-b nil)]
      ; 3. block until both subscriptions finish
      (while (or (not (realized? subscribe-a))
                 (not (realized? subscribe-b))))
      (is (= #{view-sig} (subscribed-views)))
      (is (= [subscriber-key-a subscriber-key-b] (keys (:subscribed @view-system))))
      (is (= #{view-sig} (get-in @view-system [:subscribed subscriber-key-a])))
      (is (= #{view-sig} (get-in @view-system [:subscribed subscriber-key-b])))
      (is (= #{subscriber-key-a subscriber-key-b} (get-in @view-system [:subscribers view-sig])))
      (is (= (hash view-data) (get-in @view-system [:hashes view-sig])))
      (is (contains-only? @test-sent-data
                          [{:subscriber-key subscriber-key-a
                            :view-sig       (dissoc view-sig :namespace)
                            :view-data      view-data}
                           {:subscriber-key subscriber-key-b
                            :view-sig       (dissoc view-sig :namespace)
                            :view-data      view-data}]))
      ; 4. have one of the subscribers unsubscribe
      (unsubscribe! view-sig subscriber-key-a nil)
      (is (= #{view-sig} (subscribed-views)))
      (is (= [subscriber-key-b] (keys (:subscribed @view-system))))
      (is (= #{view-sig} (get-in @view-system [:subscribed subscriber-key-b])))
      (is (= #{subscriber-key-b} (get-in @view-system [:subscribers view-sig])))
      (is (= (hash view-data) (get-in @view-system [:hashes view-sig])))
      ; 5. have the last subscriber also unsubscribe
      (unsubscribe! view-sig subscriber-key-b nil)
      (is (empty? (keys (:subscribed @view-system))))
      (is (empty? (keys (:subscribers @view-system))))
      (is (empty? (subscribed-views)))
      (is (empty? (:hashes @view-system))))))

(deftest subscriptions-to-different-views
  (let [options          default-options
        subscriber-key-a 123
        subscriber-key-b 456
        view-sig-a       (->view-sig :a :foo [])
        view-sig-b       (->view-sig :a :bar [])]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. subscribe to a view
    (let [view-data-a (get-view-data view-sig-a)
          view-data-b (get-view-data view-sig-b)
          subscribe-a (subscribe! view-sig-a subscriber-key-a nil)
          subscribe-b (subscribe! view-sig-b subscriber-key-b nil)]
      ; 3. block until both subscriptions finish
      (while (or (not (realized? subscribe-a))
                 (not (realized? subscribe-b))))
      (is (= #{view-sig-a view-sig-b} (subscribed-views)))
      (is (= [subscriber-key-a subscriber-key-b] (keys (:subscribed @view-system))))
      (is (= #{view-sig-a} (get-in @view-system [:subscribed subscriber-key-a])))
      (is (= #{view-sig-b} (get-in @view-system [:subscribed subscriber-key-b])))
      (is (= #{subscriber-key-a} (get-in @view-system [:subscribers view-sig-a])))
      (is (= #{subscriber-key-b} (get-in @view-system [:subscribers view-sig-b])))
      (is (= (hash view-data-a) (get-in @view-system [:hashes view-sig-a])))
      (is (= (hash view-data-b) (get-in @view-system [:hashes view-sig-b])))
      (is (contains-only? @test-sent-data
                          [{:subscriber-key subscriber-key-a
                            :view-sig       (dissoc view-sig-a :namespace)
                            :view-data      view-data-a}
                           {:subscriber-key subscriber-key-b
                            :view-sig       (dissoc view-sig-b :namespace)
                            :view-data      view-data-b}]))
      ; 4. have one of the subscribers unsubscribe
      (unsubscribe! view-sig-a subscriber-key-a nil)
      (is (= #{view-sig-b} (subscribed-views)))
      (is (= [subscriber-key-b] (keys (:subscribed @view-system))))
      (is (empty? (get-in @view-system [:subscribed subscriber-key-a])))
      (is (= #{view-sig-b} (get-in @view-system [:subscribed subscriber-key-b])))
      (is (= #{subscriber-key-b} (get-in @view-system [:subscribers view-sig-b])))
      (is (empty? (get-in @view-system [:subscribers view-sig-a])))
      (is (empty? (get-in @view-system [:hashes view-sig-a])))
      (is (= (hash view-data-b) (get-in @view-system [:hashes view-sig-b])))
      ; 5. have the last subscriber also unsubscribe
      (unsubscribe! view-sig-b subscriber-key-b nil)
      (is (empty? (keys (:subscribed @view-system))))
      (is (empty? (keys (:subscribers @view-system))))
      (is (empty? (subscribed-views)))
      (is (empty? (:hashes @view-system))))))

(deftest duplicate-subscriptions-do-not-cause-problems
  (let [options        default-options
        subscriber-key 123
        view-sig       (->view-sig :a :foo [])]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. subscribe to a view
    (let [view-data        (get-view-data view-sig)
          first-subscribe  (subscribe! view-sig subscriber-key nil)
          second-subscribe (subscribe! view-sig subscriber-key nil)]
      ; 3. block until both subscriptions finish
      (while (or (not (realized? first-subscribe))
                 (not (realized? second-subscribe))))
      (is (= #{view-sig} (subscribed-views)))
      (is (= [subscriber-key] (keys (:subscribed @view-system))))
      (is (= #{view-sig} (get-in @view-system [:subscribed subscriber-key])))
      (is (= #{subscriber-key} (get-in @view-system [:subscribers view-sig])))
      (is (= (hash view-data) (get-in @view-system [:hashes view-sig])))
      (is (contains-only? @test-sent-data
                          [{:subscriber-key subscriber-key
                            :view-sig       (dissoc view-sig :namespace)
                            :view-data      view-data}
                           {:subscriber-key subscriber-key
                            :view-sig       (dissoc view-sig :namespace)
                            :view-data      view-data}]))
      ; 4. unsubscribe. only need to do this once, since only one subscription
      ;    should exist in the view system
      (unsubscribe! view-sig subscriber-key nil)
      (is (empty? (keys (:subscribed @view-system))))
      (is (empty? (keys (:subscribers @view-system))))
      (is (empty? (subscribed-views)))
      (is (empty? (:hashes @view-system))))))

(deftest subscribing-to-non-existant-view-raises-exception
  (let [options        default-options
        subscriber-key 123
        view-sig       (->view-sig :namespace :non-existant-view [])]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. subscribe to a view
    (is (thrown? Exception (subscribe! view-sig subscriber-key nil)))))

(deftest subscribe-and-unsubscribe-use-namespace-fn-if-set-and-no-namespace-in-view-sig
  (let [subscriber-key 123
        view-sig       (->view-sig :foo [])
        context        "some arbitrary context data"
        namespace-fn   (fn [view-sig* subscriber-key* context*]
                         (is (= view-sig view-sig*))
                         (is (= subscriber-key subscriber-key*))
                         (is (= context context*))
                         :b)
        options        (-> default-options
                           (assoc :namespace-fn namespace-fn))]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. subscribe to a view
    (let [; with the above namespace-fn, subscribe will internally use this view sig
          ; when setting up subscription info within view-system. application code
          ; shouldn't need to worry about this, but we will in this unit test
          view-sig-with-ns (->view-sig :b :foo [])
          ; such as right here, we need to use the actual namespace that was set in
          ; view-system to pass in the same parameters that subscribe! will use for
          ; the view during it's initial view data refresh
          view-data        (get-view-data view-sig-with-ns)
          ; passing in view-sig *without* namespace
          subscribe-result (subscribe! view-sig subscriber-key context)]
      ; 3. block until subscription finishes
      (while (not (realized? subscribe-result)))
      (is (= #{view-sig-with-ns} (subscribed-views)))
      (is (= [subscriber-key] (keys (:subscribed @view-system))))
      (is (= #{view-sig-with-ns} (get-in @view-system [:subscribed subscriber-key])))
      (is (= #{subscriber-key} (get-in @view-system [:subscribers view-sig-with-ns])))
      (is (= (hash view-data) (get-in @view-system [:hashes view-sig-with-ns])))
      (is (contains-only? @test-sent-data
                          [{:subscriber-key subscriber-key
                            :view-sig       (dissoc view-sig :namespace)
                            :view-data      view-data}]))
      ; 4. unsubscribe.
      ; NOTE: we are passing in view-sig, not view-sig-with-ns. this is because
      ;       proper namespace-fn's should be consistent with what namespace they
      ;       return given the same inputs. ideal namespace-fn implementations will
      ;       also keep this in mind even if context could vary between subscribe!
      ;       and unsubscribe! calls.
      (unsubscribe! view-sig subscriber-key context)
      (is (empty? (keys (:subscribed @view-system))))
      (is (empty? (keys (:subscribers @view-system))))
      (is (empty? (subscribed-views)))
      (is (empty? (:hashes @view-system))))))

(deftest subscribe-and-unsubscribe-do-not-use-namespace-fn-if-namespace-included-in-view-sig
  (let [subscriber-key 123
        view-sig       (->view-sig :a :foo [])
        context        "some arbitrary context data"
        namespace-fn   (fn [view-sig* subscriber-key* context*]
                         ; if this function is used, it will mess up several assertions in this unit test
                         :b)
        options        (-> default-options
                           (assoc :namespace-fn namespace-fn))]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. subscribe to a view
    (let [view-data        (get-view-data view-sig)
          subscribe-result (subscribe! view-sig subscriber-key context)]
      ; 3. block until subscription finishes
      (while (not (realized? subscribe-result)))
      (is (= #{view-sig} (subscribed-views)))
      (is (= [subscriber-key] (keys (:subscribed @view-system))))
      (is (= #{view-sig} (get-in @view-system [:subscribed subscriber-key])))
      (is (= #{subscriber-key} (get-in @view-system [:subscribers view-sig])))
      (is (= (hash view-data) (get-in @view-system [:hashes view-sig])))
      (is (contains-only? @test-sent-data
                          [{:subscriber-key subscriber-key
                            :view-sig       (dissoc view-sig :namespace)
                            :view-data      view-data}]))
      ; 4. unsubscribe.
      (unsubscribe! view-sig subscriber-key context)
      (is (empty? (keys (:subscribed @view-system))))
      (is (empty? (keys (:subscribers @view-system))))
      (is (empty? (subscribed-views)))
      (is (empty? (:hashes @view-system))))))

(deftest unauthorized-subscription-using-auth-fn
  (let [subscriber-key 123
        view-sig       (->view-sig :a :foo [])
        context        "some arbitrary context data"
        auth-fn        (fn [view-sig* subscriber-key* context*]
                         (is (= view-sig view-sig*))
                         (is (= subscriber-key subscriber-key*))
                         (is (= context context*))
                         ; false = unauthorized
                         false)
        options        (-> default-options
                           (assoc :auth-fn auth-fn))]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. subscribe to a view
    (let [subscribe-result (subscribe! view-sig subscriber-key context)]
      (is (nil? subscribe-result))
      (is (empty? (keys (:subscribed @view-system))))
      (is (empty? (keys (:subscribers @view-system))))
      (is (empty? (subscribed-views)))
      (is (empty? (:hashes @view-system))))))

(deftest unauthorized-subscription-using-auth-fn-calls-on-unauth-fn-when-set
  (let [subscriber-key    123
        view-sig          (->view-sig :a :foo [])
        context           "some arbitrary context data"
        auth-fn           (fn [view-sig* subscriber-key* context*]
                            (is (= view-sig view-sig*))
                            (is (= subscriber-key subscriber-key*))
                            (is (= context context*))
                            ; false = unauthorized
                            false)
        on-unauth-called? (atom false)
        on-unauth-fn      (fn [view-sig* subscriber-key* context*]
                            (is (= view-sig view-sig*))
                            (is (= subscriber-key subscriber-key*))
                            (is (= context context*))
                            (reset! on-unauth-called? true))
        options           (-> default-options
                              (assoc :auth-fn auth-fn
                                     :on-unauth-fn on-unauth-fn))]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. subscribe to a view
    (let [subscribe-result (subscribe! view-sig subscriber-key context)]
      (is (nil? subscribe-result))
      (is @on-unauth-called?)
      (is (empty? (keys (:subscribed @view-system))))
      (is (empty? (keys (:subscribers @view-system))))
      (is (empty? (subscribed-views)))
      (is (empty? (:hashes @view-system))))))

(deftest authorized-subscription-using-auth-fn
  (let [subscriber-key 123
        view-sig       (->view-sig :a :foo [])
        context        "some arbitrary context data"
        auth-fn        (fn [view-sig* subscriber-key* context*]
                         (is (= view-sig view-sig*))
                         (is (= subscriber-key subscriber-key*))
                         (is (= context context*))
                         true)
        options        (-> default-options
                           (assoc :auth-fn auth-fn))]
    ; 1. init views
    (init! views test-send-fn options)
    ; 2. subscribe to a view
    (let [view-data        (get-view-data view-sig)
          subscribe-result (subscribe! view-sig subscriber-key context)]
      (while (not (realized? subscribe-result)))
      (is (= #{view-sig} (subscribed-views)))
      (is (= [subscriber-key] (keys (:subscribed @view-system))))
      (is (= #{view-sig} (get-in @view-system [:subscribed subscriber-key])))
      (is (= #{subscriber-key} (get-in @view-system [:subscribers view-sig])))
      (is (= (hash view-data) (get-in @view-system [:hashes view-sig])))
      (is (contains-only? @test-sent-data
                          [{:subscriber-key subscriber-key
                            :view-sig       (dissoc view-sig :namespace)
                            :view-data      view-data}])))))

(deftest unsubscribe-before-subscription-finishes-does-not-result-in-stuck-view
  (let [subscriber-key 123
        view-sig       (->view-sig :a :foo [])
        options        default-options]
    ; 1. init views
    (init! slow-views test-send-fn options)
    ; 2. subscribe to a view
    (let [subscribe-result (subscribe! view-sig subscriber-key nil)]
      (is (= #{view-sig} (subscribed-views)))
      (is (not (realized? subscribe-result)))
      ; 3. unsubscribe before subscription finishes (still waiting on initial data
      ;    retrieval to finish)
      (unsubscribe! view-sig subscriber-key nil)
      (is (empty? (keys (:subscribed @view-system))))
      (is (empty? (keys (:subscribers @view-system))))
      (is (empty? (subscribed-views)))
      (is (empty? (:hashes @view-system)))
      (is (empty? @test-sent-data))
      ; 4. wait for subscription to finish finally
      (while (not (realized? subscribe-result)))
      (is (empty? (keys (:subscribed @view-system))))
      (is (empty? (keys (:subscribers @view-system))))
      (is (empty? (subscribed-views)))
      (is (empty? (:hashes @view-system)))
      (is (empty? @test-sent-data)))))
