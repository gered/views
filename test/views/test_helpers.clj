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
