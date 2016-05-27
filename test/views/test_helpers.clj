(ns views.test-helpers
  (:use
    clojure.test
    views.protocols
    views.core))

(defn contains-view?
  [view-id]
  (let [view (get (:views @view-system) view-id)]
    (and view
         (satisfies? IView view))))

; the purpose of this function is to compare collections when the order of the elements
; is not important, but there could be duplicates (so a set is not being used). some of
; the operations we test are asynchronous with multiple threads performing the same
; operation simultaneously, so at times our tests that record these operations being done
; could end up with collections of items that are out of order between test runs.
; this is not a problem or bug in views, but just a consequence of the multithreaded
; operation of the library.
(defn contains-only?
  [coll elements]
  (and (= (count coll)
          (count elements))
       (every?
         #(boolean (some #{%} elements))
         coll)
       (every?
         #(boolean (some #{%} coll))
         elements)))

(defn get-view-data
  [view-sig]
  (data (get-in @view-system [:views (:view-id view-sig)])
        (:namespace view-sig)
        (:parameters view-sig)))

; the 200 being used is just a number i pulled out of thin air that "felt good"

(defn wait-for-refresh-views []
  (Thread/sleep 200))

(defn wait-for-refresh-interval [options]
  (Thread/sleep (+ 200 (:refresh-interval options))))

; this is kind of a hack, but necessary for some tests where we want to inspect
; the items being sent to the refresh queue without worker threads picking out
; the added items almost instantly.
(defn stop-refresh-worker-threads []
  (swap! view-system assoc :stop-workers? true)
  (doseq [^Thread t (:workers @view-system)]
    (.interrupt t)
    (.join t))
  (swap! view-system assoc :workers nil))
