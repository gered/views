(ns views.test-memory-db
  (:use
    views.protocols
    views.core))

(def memory-database
  (atom {:a {:foo 1 :bar 200 :baz [1 2 3]}
         :b {:foo 2 :bar 300 :baz [2 3 4]}}))

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

(defrecord SlowMemoryView [id ks]
  IView
  (id [_] id)
  (data [_ namespace parameters]
    ; simulate a slow database query
    (Thread/sleep 1000)
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

(def slow-views
  [(SlowMemoryView. :foo [:foo])
   (SlowMemoryView. :bar [:bar])
   (SlowMemoryView. :baz [:baz])])
