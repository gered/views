(ns views.core
  (:import
    [java.util.concurrent ArrayBlockingQueue TimeUnit])
  (:require
    [views.protocols :refer [IView id data relevant?]]
    [plumbing.core :refer [swap-pair!]]
    [clojure.tools.logging :refer [info debug error]]
    [environ.core :refer [env]]))

;; The view-system data structure has this shape:
;;
;; {:views {:id1 view1, id2 view2, ...}
;;  :send-fn (fn [subscriber-key data] ...)
;;  :put-hints-fn (fn [hints] ... )
;;  :auth-fn (fn [view-sig subscriber-key context] ...)
;;
;;  :hashes {view-sig hash, ...}
;;  :subscribed {subscriber-key #{view-sig, ...}}
;;  :subscribers {view-sig #{subscriber-key, ...}}
;;  :hints #{hint1 hint2 ...}
;;
;;  }
;;
;;  Each hint has the form {:namespace x :hint y}

(defonce view-system (atom {}))



(def refresh-queue-size
  (if-let [n (:views-refresh-queue-size env)]
    (Long/parseLong n)
    1000))

(defonce statistics (atom {}))

(defn reset-stats!
  []
  (swap! statistics (fn [s] {:enabled (boolean (:enabled s)), :refreshes 0, :dropped 0, :deduplicated 0})))

(defn collect-stats? [] (:enabled @statistics))

(reset-stats!)


(def refresh-queue (ArrayBlockingQueue. refresh-queue-size))

(defn ->view-sig
  [namespace view-id parameters]
  {:namespace  namespace
   :view-id    view-id
   :parameters parameters})

(defn- send-view-data!
  [subscriber-key {:keys [namespace view-id parameters] :as view-sig} data]
  (if-let [send-fn (:send-fn @view-system)]
    (send-fn subscriber-key [(dissoc view-sig :namespace) data])
    (throw (new Exception "no send-fn function set in view-system"))))

(defn- authorized-subscription?
  [view-sig subscriber-key context]
  (if-let [auth-fn (:auth-fn @view-system)]
    (auth-fn view-sig subscriber-key context)
    ; assume that if no auth-fn is specified, that we are not doing auth checks at all
    ; so do not disallow access to any subscription
    true))

(defn- subscribe-view!
  [view-system view-sig subscriber-key]
  (-> view-system
      (update-in [:subscribed subscriber-key] (fnil conj #{}) view-sig)
      (update-in [:subscribers view-sig] (fnil conj #{}) subscriber-key)))

(defn- update-hash!
  [view-system view-sig data-hash]
  (update-in view-system [:hashes view-sig] #(or % data-hash))) ;; see note #1 in NOTES.md

(defn subscribe!
  [namespace view-id parameters subscriber-key & [context]]
  (when-let [view (get-in @view-system [:views view-id])]
    (let [view-sig (->view-sig namespace view-id parameters)]
      (if (authorized-subscription? view-sig subscriber-key context)
        (do
          (swap! view-system subscribe-view! view-sig subscriber-key)
          (future
            (try
              (let [vdata     (data view namespace parameters)
                    data-hash (hash vdata)]
                ;; Check to make sure that we are still subscribed. It's possible that
                ;; an unsubscription event came in while computing the view.
                (when (contains? (get-in @view-system [:subscribed subscriber-key]) view-sig)
                  (swap! view-system update-hash! view-sig data-hash)
                  (send-view-data! subscriber-key view-sig vdata)))
              (catch Exception e
                (error "error subscribing:" namespace view-id parameters
                       "e:" e "msg:" (.getMessage e))))))
        (debug "subscription not authorized" view-sig subscriber-key context)))))

(defn- remove-from-subscribers
  [view-system view-sig subscriber-key]
  (-> view-system
      (update-in [:subscribers view-sig] disj subscriber-key)
      ; remove view-sig entry if no subscribers. helps prevent the subscribers
      ; map from e.g. endlessly filling up with all sorts of different
      ; view-sigs with crazy amounts of only-slightly-varying parameters
      (update-in [:subscribers]
                 (fn [subscribers]
                   (if (empty? (get subscribers view-sig))
                     (dissoc subscribers view-sig)
                     subscribers)))))

(defn- remove-from-subscribed
  [view-system view-sig subscriber-key]
  (-> view-system
      (update-in [:subscribed subscriber-key] disj view-sig)
      ; remove subscriber-key entry if no current subscriptions. this helps prevent
      ; the subscribed map from (for example) endlessly filling up with massive
      ; amounts of entries with no subscriptions. this could easily happen over time
      ; naturally for applications with long uptimes.
      (update-in [:subscribed]
                 (fn [subscribed]
                   (if (empty? (get subscribed subscriber-key))
                     (dissoc subscribed subscriber-key)
                     subscribed)))))

(defn- clean-up-unneeded-hashes
  [view-system view-sig]
  ; hashes for view-sigs which do not have any unsubscribers are no longer necessary
  ; to keep around (again, at risk of endlessly filling up with tons of hashes over time)
  (if-not (get (:subscribers view-system) view-sig)
    (update-in view-system [:hashes] dissoc view-sig)
    view-system))

(defn unsubscribe!
  [namespace view-id parameters subscriber-key]
  (swap! view-system
         (fn [vs]
           (let [view-sig (->view-sig namespace view-id parameters)]
             (-> vs
                 (remove-from-subscribed view-sig subscriber-key)
                 (remove-from-subscribers view-sig subscriber-key)
                 (clean-up-unneeded-hashes view-sig))))))

(defn unsubscribe-all!
  "Remove all subscriptions by a given subscriber."
  [subscriber-key]
  (swap! view-system
         (fn [vs]
           (let [view-sigs (get-in vs [:subscribed subscriber-key])
                 vs*       (update-in vs [:subscribed] dissoc subscriber-key)]
             (reduce #(remove-from-subscribers %1 %2 subscriber-key) vs* view-sigs)))))

(defn refresh-view!
  "We refresh a view if it is relevant and its data hash has changed."
  [hints {:keys [namespace view-id parameters] :as view-sig}]
  (let [v (get-in @view-system [:views view-id])]
    (try
      (if (relevant? v namespace parameters hints)
        (if-not (.contains ^ArrayBlockingQueue refresh-queue view-sig)
          (when-not (.offer ^ArrayBlockingQueue refresh-queue view-sig)
            (when (collect-stats?) (swap! statistics update-in [:dropped] inc))
            (error "refresh-queue full, dropping refresh request for" view-sig))
          (do
            (when (collect-stats?) (swap! statistics update-in [:deduplicated] inc))
            (debug "already queued for refresh" view-sig))))
      (catch Exception e (error "error determining if view is relevant, view-id:"
                                view-id "e:" e)))))

(defn subscribed-views
  []
  (reduce into #{} (vals (:subscribed @view-system))))

(defn active-view-count
  "Returns a count of views with at least one subscriber."
  []
  (count (remove #(empty? (val %)) (:subscribers @view-system))))

(defn pop-hints!
  "Return hints and clear hint set atomicly."
  []
  (let [p (swap-pair! view-system assoc :hints #{})]
    (or (:hints (first p)) #{})))

(defn refresh-views!
  "Given a collection of hints, or a single hint, find all dirty views and schedule them for a refresh."
  ([hints]
   (when (seq hints)
     (debug "refresh hints:" hints)
     (mapv #(refresh-view! hints %) (subscribed-views)))
   (swap! view-system assoc :last-update (System/currentTimeMillis)))
  ([]
   (refresh-views! (pop-hints!))))

(defn can-refresh?
  [last-update min-refresh-interval]
  (> (- (System/currentTimeMillis) last-update) min-refresh-interval))

(defn wait
  [last-update min-refresh-interval]
  (Thread/sleep (max 0 (- min-refresh-interval (- (System/currentTimeMillis) last-update)))))

(defn refresh-worker-thread
  "Handles refresh requests."
  []
  (fn []
    (try
      (when-let [{:keys [namespace view-id parameters] :as view-sig} (.poll ^ArrayBlockingQueue refresh-queue 60 TimeUnit/SECONDS)]
        (when (collect-stats?) (swap! statistics update-in [:refreshes] inc))
        (try
          (let [view  (get-in @view-system [:views view-id])
                vdata (data view namespace parameters)
                hdata (hash vdata)]
            (when-not (= hdata (get-in @view-system [:hashes view-sig]))
              (doseq [subscriber-key (get-in @view-system [:subscribers view-sig])]
                (send-view-data! subscriber-key view-sig vdata))
              (swap! view-system assoc-in [:hashes view-sig] hdata)))
          (catch Exception e
            (error "error refreshing:" namespace view-id parameters
                   "e:" e "msg:" (.getMessage e)))))
      (catch InterruptedException e))
    (if-not (:stop-workers? @view-system)
      (recur)
      (debug "exiting worker thread"))))

(defn refresh-watcher-thread
  [min-refresh-interval]
  (fn []
    (let [last-update (:last-update @view-system)]
      (try
        (if (can-refresh? last-update min-refresh-interval)
          (refresh-views!)
          (wait last-update min-refresh-interval))
        (catch InterruptedException e)
        (catch Exception e
          (error "exception in views e:" e "msg:" (.getMessage e))))
      (if-not (:stop-refresh-watcher? @view-system)
        (recur)
        (debug "exiting refresh watcher thread")))))

(defn start-update-watcher!
  "Starts threads for the views refresh watcher and worker threads that handle
   view refresh requests."
  [min-refresh-interval threads]
  (if (and (:refresh-watcher @view-system)
           (:workers @view-system))
    (error "cannot start new watcher and worker threads until existing threads are stopped")
    (let [refresh-watcher (Thread. ^Runnable (refresh-watcher-thread min-refresh-interval))
          worker-threads  (mapv (fn [_] (Thread. ^Runnable (refresh-worker-thread)))
                                (range threads))]
      (swap! view-system assoc
             :last-update 0
             :refresh-watcher refresh-watcher
             :stop-refresh-watcher? false
             :workers worker-threads
             :stop-workers? false)
      (.start refresh-watcher)
      (doseq [^Thread t worker-threads]
        (.start t)))))

(defn stop-update-watcher!
  "Stops threads for the views refresh watcher and worker threads."
  []
  (swap! view-system assoc
         :stop-refresh-watcher? true
         :stop-workers? true)
  (if-let [^Thread refresh-watcher (:refresh-watcher @view-system)]
    (.interrupt refresh-watcher))
  (doseq [^Thread worker-thread (:workers @view-system)]
    (.interrupt worker-thread))
  (swap! view-system assoc
         :refresh-watcher nil
         :workers nil))

(defn log-statistics!
  "Run a thread that logs statistics every msecs."
  [msecs]
  (swap! statistics assoc-in [:enabled] true)
  (let [secs (/ msecs 1000)]
    (.start (Thread. (fn []
                       (Thread/sleep msecs)
                       (let [stats @statistics]
                         (reset-stats!)
                         (info "subscribed views:" (active-view-count)
                               (format "refreshes/sec: %.1f" (double (/ (:refreshes stats) secs)))
                               (format "dropped/sec: %.1f" (double (/ (:dropped stats) secs)))
                               (format "deduped/sec: %.1f" (double (/ (:deduplicated stats) secs))))
                         (recur)))))))

(defn hint
  "Create a hint."
  [namespace hint]
  {:namespace namespace :hint hint})

(defn add-hint!
  "Add a hint to the system."
  [hint]
  (swap! view-system update-in [:hints] (fnil conj #{}) hint))

(defn add-views!
  "Add a collection of views to the system."
  [views]
  (swap! view-system update-in [:views] (fnil into {}) (map vector (map id views) views)))

(defn set-send-fn!
  "Sets a function that sends view data to a subscriber whenever a view it
   is subscribed to has refreshed data."
  [f]
  (swap! view-system assoc :send-fn f))

(defn set-put-hints-fn!
  "Sets a function that adds hints to the view system. The function set is intended
   to be used by other implementations of IView."
  [f]
  (swap! view-system assoc :put-hints-fn f))

(defn set-auth-fn!
  "Sets a function that authorizes view subscriptions. If authorization fails
   (the function returns false), the subscription is not processed."
  [f]
  (swap! view-system assoc :auth-fn f))

(defn init!
  "Initializes the view system for use with some basic defaults that can be
   overridden as needed. Many applications may want to ignore this function
   and instead manually initialize the view system themselves. Some of the
   defaults set by this function are only appropriate for non-distributed
   configurations."
  [& {:keys [refresh-interval worker-threads send-fn put-hints-fn auth-fn views]
      :or   {refresh-interval 1000
             worker-threads   4
             put-hints-fn     #(refresh-views! %)}}]
  (if send-fn (set-send-fn! send-fn))
  (if put-hints-fn (set-put-hints-fn! put-hints-fn))
  (if auth-fn (set-auth-fn! auth-fn))
  (if views (add-views! views))
  (start-update-watcher! refresh-interval worker-threads))

(defn shutdown!
  "Closes the view system down, terminating all worker threads and clearing
   all view subscriptions and data."
  []
  (stop-update-watcher!)
  (reset! view-system {}))

(comment
  (defrecord SQLView [id query-fn]
    IView
    (id [_] id)
    (data [_ namespace parameters]
      (j/query (db/firm-connection namespace) (hsql/format (apply query-fn parameters))))
    (relevant? [_ namespace parameters hints]
      (let [tables (query-tables (apply query-fn parameters))]
        (boolean (some #(not-empty (intersection % talbes)) hints)))))

  (reset! in-memory-data {:a {:foo 1 :bar 200 :baz [1 2 3]}
                          :b {:foo 2 :bar 300 :baz [2 3 4]}})

  (defrecord MemoryView [id ks]
    IView
    (id [_] id)
    (data [_ namespace parameters]
      (get-in @in-memory-data (-> [namespace] (into ks) (into parameters))))
    (relevant? [_ namespace parameters hints]
      (some #(and (= namespace (:namespace %)) (= ks (:hint %))) hints)))

  (reset! view-system
          {:views   {:foo (MemoryView. :foo [:foo])
                     :bar (MemoryView. :bar [:bar])
                     :baz (MemoryView. :baz [:baz])}
           :send-fn (fn [subscriber-key data] (println "sending to:" subscriber-key "data:" data))})

  (subscribe! :a :foo [] 1)
  (subscribe! :b :foo [] 2)
  (subscribe! :b :baz [] 2)

  (subscribed-views)

  (add-hint! [:foo])
  (add-hint! [:baz])

  (refresh-views!)

  ;; Example of function that updates and hints the view system.
  (defn massoc-in!
    [memory-db namespace ks v]
    (let [ms (swap! memory-db assoc-in (into [namespace] ks) v)]
      (add-hint! ks)
      ms))

  (massoc-in! in-memory-data :a [:foo] 1)
  (massoc-in! in-memory-data :b [:baz] [2 4 3])


  (start-update-watcher! 1000 1)

  )
