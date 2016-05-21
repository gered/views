(ns views.core
  (:import
    [java.util.concurrent ArrayBlockingQueue TimeUnit])
  (:require
    [views.protocols :refer [IView id data relevant?]]
    [plumbing.core :refer [swap-pair!]]
    [clojure.tools.logging :refer [info debug error trace]]))

;; The view-system data structure has this shape:
;;
;; {
;;
;;  :refresh-queue (ArrayBlockingQueue.)
;;  :views         {:id1 view1, id2 view2, ...}
;;  :send-fn       (fn [subscriber-key data] ...)
;;  :put-hints-fn  (fn [hints] ... )
;;  :auth-fn       (fn [view-sig subscriber-key context] ...)
;;  :namespace-fn  (fn [view-sig subscriber-key context] ...)
;;
;;  :hashes        {view-sig hash, ...}
;;  :subscribed    {subscriber-key #{view-sig, ...}}
;;  :subscribers   {view-sig #{subscriber-key, ...}}
;;  :hints         #{hint1 hint2 ...}
;;
;;  }
;;
;;  Each hint has the form {:namespace x :hint y}

(defonce view-system (atom {}))

(defonce statistics (atom {}))



(defn reset-stats!
  []
  (swap! statistics assoc
         :refreshes 0
         :dropped 0
         :deduplicated 0))

(defn collect-stats?
  []
  (boolean (:logger @statistics)))

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

(defn- get-namespace
  [view-sig subscriber-key context]
  (if-let [namespace-fn (:namespace-fn @view-system)]
    (namespace-fn view-sig subscriber-key context)
    (:namespace view-sig)))

(defn- subscribe-view!
  [view-system view-sig subscriber-key]
  (trace "subscribing to view" view-sig subscriber-key)
  (-> view-system
      (update-in [:subscribed subscriber-key] (fnil conj #{}) view-sig)
      (update-in [:subscribers view-sig] (fnil conj #{}) subscriber-key)))

(defn- update-hash!
  [view-system view-sig data-hash]
  (update-in view-system [:hashes view-sig] #(or % data-hash))) ;; see note #1 in NOTES.md

(defn subscribe!
  "Creates a subscription to a view identified by view-sig for a subscriber
   identified by subscriber-key. If the subscription is not authorized,
   returns nil. Additional context info can be passed in, which will be
   passed to the view-system's namespace-fn and auth-fn (if provided). If
   the subscription is successful, the subscriber will be sent the initial
   data for the view."
  [{:keys [namespace view-id parameters] :as view-sig} subscriber-key context]
  (when-let [view (get-in @view-system [:views view-id])]
    (let [namespace (get-namespace view-sig subscriber-key context)
          view-sig  (->view-sig namespace view-id parameters)]
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
        (trace "subscription not authorized" view-sig subscriber-key context)))))

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
  "Removes a subscription to a view identified by view-sig for a subscriber
   identified by subscriber-key. Additional context info can be passed in,
   which will be passed to the view-system's namespace-fn (if provided)."
  [{:keys [namespace view-id parameters] :as view-sig} subscriber-key context]
  (trace "unsubscribing from view" view-sig subscriber-key)
  (swap! view-system
         (fn [vs]
           (let [namespace (get-namespace view-sig subscriber-key context)
                 view-sig  (->view-sig namespace view-id parameters)]
             (-> vs
                 (remove-from-subscribed view-sig subscriber-key)
                 (remove-from-subscribers view-sig subscriber-key)
                 (clean-up-unneeded-hashes view-sig))))))

(defn unsubscribe-all!
  "Removes all of a subscriber's (identified by subscriber-key) current
   view subscriptions."
  [subscriber-key]
  (trace "unsubscribing from all views" subscriber-key)
  (swap! view-system
         (fn [vs]
           (let [view-sigs (get-in vs [:subscribed subscriber-key])
                 vs*       (update-in vs [:subscribed] dissoc subscriber-key)]
             (reduce #(remove-from-subscribers %1 %2 subscriber-key) vs* view-sigs)))))

(defn refresh-view!
  "Schedules a view (identified by view-sig) to be refreshed by one of the worker threads
   only if the provided collection of hints is relevant to that view."
  [hints {:keys [namespace view-id parameters] :as view-sig}]
  (let [v (get-in @view-system [:views view-id])]
    (if-let [^ArrayBlockingQueue refresh-queue (:refresh-queue @view-system)]
      (try
        (if (relevant? v namespace parameters hints)
          (if-not (.contains refresh-queue view-sig)
            (when-not (.offer refresh-queue view-sig)
              (when (collect-stats?) (swap! statistics update-in [:dropped] inc))
              (error "refresh-queue full, dropping refresh request for" view-sig))
            (do
              (when (collect-stats?) (swap! statistics update-in [:deduplicated] inc))
              (trace "already queued for refresh" view-sig))))
        (catch Exception e (error "error determining if view is relevant, view-id:"
                                  view-id "e:" e))))))

(defn subscribed-views
  "Returns a list of all views in the system that have subscribers."
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
  "Given a collection of hints, check all views in the system to find any that need refreshing
   and schedule refreshes for them. If no hints are provided, will use any that have been
   queued up in the view-system."
  ([hints]
   (when (seq hints)
     (trace "refresh hints:" hints)
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
  "Returns a refresh worker thread function. A 'refresh worker' continually waits for
   refresh requests and when there is one, handles it by running the view, getting the view
   data and then sending it out to all the view's subscribers. "
  []
  (let [^ArrayBlockingQueue refresh-queue (:refresh-queue @view-system)]
    (fn []
      (try
        (when-let [{:keys [namespace view-id parameters] :as view-sig} (.poll refresh-queue 60 TimeUnit/SECONDS)]
          (trace "worker running refresh for" view-sig)
          (if (collect-stats?) (swap! statistics update-in [:refreshes] inc))
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
        (trace "exiting worker thread")))))

(defn refresh-watcher-thread
  "Returns a refresh watcher thread function. A 'refresh watcher' continually attempts
   to schedule refreshes for any views in the system which are 'dirty' (a dirty view in
   this case is one when there is a hint waiting in the view-system that is relevant
   to the view)."
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
        (trace "exiting refresh watcher thread")))))

(defn start-update-watcher!
  "Starts threads for the views refresh watcher and worker threads that handle
   view refresh requests."
  [min-refresh-interval threads]
  (trace "starting refresh watcher at" min-refresh-interval "ms interval and" threads "workers")
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
  (trace "stopping refresh watcher and workers")
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

(defn logger-thread
  "Returns a logger thread function. A logger periodically writes view system
   statistics to the log that are collected only when logging is enabled."
  [msecs]
  (let [secs (/ msecs 1000)]
    (fn []
      (try
        (Thread/sleep msecs)
        (let [stats @statistics]
          (reset-stats!)
          (info "subscribed views:" (active-view-count)
                (format "refreshes/sec: %.1f" (double (/ (:refreshes stats) secs)))
                (format "dropped/sec: %.1f" (double (/ (:dropped stats) secs)))
                (format "deduped/sec: %.1f" (double (/ (:deduplicated stats) secs)))))
        (catch InterruptedException e))
      (if-not (:stop? @statistics)
        (recur)))))

(defn start-logger!
  "Starts a logger thread that will enable collection of view statistics
   which the logger will periodically write out to the log."
  [log-interval]
  (trace "starting logger. logging at" log-interval "secs intervals")
  (if (:logger @statistics)
    (error "cannot start new logger thread until existing thread is stopped")
    (let [logger (Thread. ^Runnable (logger-thread log-interval))]
      (swap! statistics assoc
             :logger logger
             :stop? false)
      (reset-stats!)
      (.start logger))))

(defn stop-logger!
  "Stops the logger thread."
  []
  (trace "stopping logger")
  (swap! statistics assoc :stop? true)
  (if-let [^Thread logger (:logger @statistics)]
    (.interrupt logger))
  (swap! statistics assoc :logger nil))

(defn hint
  "Create a hint."
  [namespace hint type]
  {:namespace namespace :hint hint :type type})

(defn queue-hints!
  "Queues up hints in the view system so that they will be picked up by the refresh
   watcher and dispatched to the workers resulting in view updates being sent out
   for the relevant views/subscribers."
  [hints]
  (trace "queueing hints" hints)
  (swap! view-system update-in [:hints]
         (fn [existing-hints]
           (reduce conj (or existing-hints #{}) hints))))

(defn put-hints!
  "Adds a collection of hints to the view system by using the view system
   configuration's :put-hints-fn."
  [hints]
  ((:put-hints-fn @view-system) hints))

(defn- get-views-map
  [views]
  (map vector (map id views) views))

(defn add-views!
  "Add a collection of views to the system."
  [views]
  (swap! view-system update-in [:views] (fnil into {}) (get-views-map views)))

(def default-options
  "Default options used to initialize the views system via init!"
  {
   ; the size of the queue used to hold view refresh requests for
   ; the worker threads. for very heavy systems, this can be set
   ; higher if you start to get warnings about dropped refresh requests
   :refresh-queue-size 1000

   ; interval in milliseconds at which the refresh watcher thread will
   ; check for any queued up hints and dispatch relevant view refresh
   ; updates to the worker threads.
   :refresh-interval   1000

   ; the number of refresh worker threads that poll for view refresh
   ; requests and dispatch updated view data to subscribers.
   :worker-threads     8

   ; a function that adds hints to the view system. this function will be used
   ; by other libraries that implement IView. this function must be set for
   ; normal operation of the views system. the default function provided
   ; will trigger relevant view refreshes immediately.
   ; (fn [hints] ... )
   :put-hints-fn       (fn [hints] (refresh-views! hints))

   ; a function that authorizes view subscriptions. should return true if the
   ; subscription is authorized. if not set, no view subscriptions will require
   ; any authorization.
   ; (fn [subscriber-key view-sig context] ... )
   :auth-fn            nil

   ; a function that returns a namespace to use for view subscriptions
   ; (fn [subscriber-key view-sig context] ... )
   :namespace-fn       nil

   ; interval in milliseconds at which a logger will write view system
   ; statistics to the log. if not set, the logger will be disabled.
   :stats-log-interval nil
   })

(defn init!
  "Initializes the view system for use with the list of views provided.

   send-fn is a function that sends view refresh data to subscribers. it is
   of the form: (fn [subscriber-key [view-sig view-data]] ... )

   options is a map of options to configure the view system with. See
   views.core/default-options for a description of the available options
   and the defaults that will be used for any options not provided in
   the call to init!."
  [views send-fn & [options]]
  (let [options (merge default-options options)]
    (trace "initializing views system using options:" options)
    (reset! view-system
            {:refresh-queue (ArrayBlockingQueue. (:refresh-queue-size options))
             :views         (into {} (get-views-map views))
             :send-fn       send-fn
             :put-hints-fn  (:put-hints-fn options)
             :auth-fn       (:auth-fn options)
             :namespace-fn  (:namespace-fn options)})
    (start-update-watcher! (:refresh-interval options)
                           (:worker-threads options))
    (when-let [stats-log-interval (:stats-log-interval options)]
      (swap! view-system assoc :logging? true)
      (start-logger! stats-log-interval))))

(defn shutdown!
  "Shuts the view system down, terminating all worker threads and clearing
   all view subscriptions and data."
  []
  (trace "shutting down views sytem")
  (stop-update-watcher!)
  (if (:logging? @view-system)
    (stop-logger!))
  (reset! view-system {}))
