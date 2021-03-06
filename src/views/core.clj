(ns views.core
  (:import
    (java.util.concurrent ArrayBlockingQueue TimeUnit)
    (clojure.lang Atom))
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



(defn reset-stats!
  "Resets statistics collected back to zero."
  [^Atom view-system]
  (swap! view-system update-in [:statistics] assoc
         :refreshes 0
         :dropped 0
         :deduplicated 0)
  view-system)

(defn collecting-stats?
  "Whether view statem statistics collection and logging is enabled or not."
  [^Atom view-system]
  (boolean (get-in @view-system [:statistics :logger])))

(defn ->view-sig
  ([namespace view-id parameters]
   {:namespace  namespace
    :view-id    view-id
    :parameters parameters})
  ([view-id parameters]
   {:view-id    view-id
    :parameters parameters}))

(defn- send-view-data!
  [view-system subscriber-key {:keys [namespace view-id parameters] :as view-sig} data]
  (if-let [send-fn (:send-fn view-system)]
    (send-fn subscriber-key [(dissoc view-sig :namespace) data])
    (throw (new Exception "no send-fn function set in view-system"))))

(defn- authorized-subscription?
  [view-system view-sig subscriber-key context]
  (if-let [auth-fn (:auth-fn view-system)]
    (auth-fn view-sig subscriber-key context)
    ; assume that if no auth-fn is specified, that we are not doing auth checks at all
    ; so do not disallow access to any subscription
    true))

(defn- on-unauthorized-subscription
  [view-system view-sig subscriber-key context]
  (if-let [on-unauth-fn (:on-unauth-fn view-system)]
    (on-unauth-fn view-sig subscriber-key context)))

(defn- get-namespace
  [view-system view-sig subscriber-key context]
  (if-let [namespace-fn (:namespace-fn view-system)]
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
  [^Atom view-system {:keys [namespace view-id parameters] :as view-sig} subscriber-key context]
  (if-let [view (get-in @view-system [:views view-id])]
    (let [namespace (if (contains? view-sig :namespace)
                      namespace
                      (get-namespace @view-system view-sig subscriber-key context))
          view-sig  (->view-sig namespace view-id parameters)]
      (if (authorized-subscription? @view-system view-sig subscriber-key context)
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
                  (send-view-data! @view-system subscriber-key view-sig vdata)))
              (catch Exception e
                (error e "error subscribing to view" view-sig)))))
        (do
          (trace "subscription not authorized" view-sig subscriber-key context)
          (on-unauthorized-subscription @view-system view-sig subscriber-key context)
          nil)))
    (throw (new Exception (str "Subscription for non-existant view: " view-id)))))

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
  [^Atom view-system {:keys [namespace view-id parameters] :as view-sig} subscriber-key context]
  (trace "unsubscribing from view" view-sig subscriber-key)
  (swap! view-system
         (fn [view-system]
           (let [namespace (if (contains? view-sig :namespace)
                             namespace
                             (get-namespace view-system view-sig subscriber-key context))
                 view-sig  (->view-sig namespace view-id parameters)]
             (-> view-system
                 (remove-from-subscribed view-sig subscriber-key)
                 (remove-from-subscribers view-sig subscriber-key)
                 (clean-up-unneeded-hashes view-sig)))))
  view-system)

(defn unsubscribe-all!
  "Removes all of a subscriber's (identified by subscriber-key) current
   view subscriptions."
  [^Atom view-system subscriber-key]
  (trace "unsubscribing from all views" subscriber-key)
  (swap! view-system
         (fn [view-system]
           (let [view-sigs    (get-in view-system [:subscribed subscriber-key])
                 view-system* (update-in view-system [:subscribed] dissoc subscriber-key)]
             (reduce
               #(-> %1
                    (remove-from-subscribers %2 subscriber-key)
                    (clean-up-unneeded-hashes %2))
               view-system*
               view-sigs))))
  view-system)

(defn refresh-view!
  "Schedules a view (identified by view-sig) to be refreshed by one of the worker threads
   only if the provided collection of hints is relevant to that view."
  [^Atom view-system hints {:keys [namespace view-id parameters] :as view-sig}]
  (let [v (get-in @view-system [:views view-id])]
    (if-let [^ArrayBlockingQueue refresh-queue (:refresh-queue @view-system)]
      (try
        (if (relevant? v namespace parameters hints)
          (if-not (.contains refresh-queue view-sig)
            (when-not (.offer refresh-queue view-sig)
              (if (collecting-stats? view-system) (swap! view-system update-in [:statistics :dropped] inc))
              (error "refresh-queue full, dropping refresh request for" view-sig))
            (do
              (if (collecting-stats? view-system) (swap! view-system update-in [:statistics :deduplicated] inc))
              (trace "already queued for refresh" view-sig))))
        (catch Exception e
          (error e "error determining if view is relevant" view-sig))))
    view-system))

(defn subscribed-views
  "Returns a list of all views in the system that have subscribers."
  [^Atom view-system]
  (reduce into #{} (vals (:subscribed @view-system))))

(defn active-view-count
  "Returns a count of views with at least one subscriber."
  [^Atom view-system]
  (count (remove #(empty? (val %)) (:subscribers @view-system))))

(defn- pop-hints!
  [^Atom view-system]
  (let [p (swap-pair! view-system assoc :hints #{})]
    (or (:hints (first p)) #{})))

(defn refresh-views!
  "Given a collection of hints, check all views in the system to find any that need refreshing
   and schedule refreshes for them. If no hints are provided, will use any that have been
   queued up in the view-system."
  ([^Atom view-system hints]
   (when (seq hints)
     (trace "refresh hints:" hints)
     (doseq [view-sig (subscribed-views view-system)]
       (refresh-view! view-system hints view-sig)))
   (swap! view-system assoc :last-update (System/currentTimeMillis))
   view-system)
  ([^Atom view-system]
   (refresh-views! view-system (pop-hints! view-system))))

(defn- can-refresh?
  [last-update min-refresh-interval]
  (> (- (System/currentTimeMillis) last-update) min-refresh-interval))

(defn- wait
  [last-update min-refresh-interval]
  (Thread/sleep (max 0 (- min-refresh-interval (- (System/currentTimeMillis) last-update)))))

(defn do-view-refresh!
  [^Atom view-system {:keys [namespace view-id parameters] :as view-sig}]
  (if (collecting-stats? view-system) (swap! view-system update-in [:statistics :refreshes] inc))
  (try
    (let [view  (get-in @view-system [:views view-id])
          vdata (data view namespace parameters)
          hdata (hash vdata)]
      (when-not (= hdata (get-in @view-system [:hashes view-sig]))
        (doseq [subscriber-key (get-in @view-system [:subscribers view-sig])]
          (send-view-data! @view-system subscriber-key view-sig vdata))
        (swap! view-system assoc-in [:hashes view-sig] hdata)))
    (catch Exception e
      (error e "error refreshing:" namespace view-id parameters))))

(defn- refresh-worker-thread
  [^Atom view-system]
  (let [^ArrayBlockingQueue refresh-queue (:refresh-queue @view-system)]
    (fn []
      (try
        (when-let [view-sig (.poll refresh-queue 60 TimeUnit/SECONDS)]
          (trace "worker running refresh for" view-sig)
          (do-view-refresh! view-system view-sig))
        (catch InterruptedException e))
      (if-not (:stop-workers? @view-system)
        (recur)
        (trace "exiting worker thread")))))

(defn- refresh-watcher-thread
  [^Atom view-system min-refresh-interval]
  (fn []
    (let [last-update (:last-update @view-system)]
      (try
        (if (can-refresh? last-update min-refresh-interval)
          (refresh-views! view-system)
          (wait last-update min-refresh-interval))
        (catch InterruptedException e)
        (catch Exception e
          (error e "exception in views")))
      (if-not (:stop-refresh-watcher? @view-system)
        (recur)
        (trace "exiting refresh watcher thread")))))

(defn start-update-watcher!
  "Starts threads for the views refresh watcher and worker threads that handle queued
   hints and view refresh requests."
  [^Atom view-system min-refresh-interval threads]
  (trace "starting refresh watcher at" min-refresh-interval "ms interval and" threads "workers")
  (if (and (:refresh-watcher @view-system)
           (:workers @view-system))
    (error "cannot start new watcher and worker threads until existing threads are stopped")
    (let [refresh-watcher (Thread. ^Runnable (refresh-watcher-thread view-system min-refresh-interval))
          worker-threads  (mapv (fn [_] (Thread. ^Runnable (refresh-worker-thread view-system)))
                                (range threads))]
      (swap! view-system assoc
             :last-update 0
             :refresh-watcher refresh-watcher
             :stop-refresh-watcher? false
             :workers worker-threads
             :stop-workers? false)
      (.start refresh-watcher)
      (doseq [^Thread t worker-threads]
        (.start t))
      view-system)))

(defn stop-update-watcher!
  "Stops threads for the views refresh watcher and worker threads."
  [^Atom view-system & [dont-wait-for-threads?]]
  (trace "stopping refresh watcher and workers")
  (let [worker-threads (:workers @view-system)
        watcher-thread (:refresh-watcher @view-system)
        threads        (->> worker-threads
                            (cons watcher-thread)
                            (remove nil?))]
    (swap! view-system assoc
           :stop-refresh-watcher? true
           :stop-workers? true)
    (doseq [^Thread t threads]
      (.interrupt t))
    (if-not dont-wait-for-threads?
      (doseq [^Thread t threads]
        (.join t)))
    (swap! view-system assoc
           :refresh-watcher nil
           :workers nil))
  view-system)

(defn- logger-thread
  [^Atom view-system msecs]
  (let [secs (/ msecs 1000)]
    (fn []
      (try
        (Thread/sleep msecs)
        (let [stats (:statistics @view-system)]
          (reset-stats! view-system)
          (info "subscribed views:" (active-view-count view-system)
                (format "refreshes/sec: %.1f" (double (/ (:refreshes stats) secs)))
                (format "dropped/sec: %.1f" (double (/ (:dropped stats) secs)))
                (format "deduped/sec: %.1f" (double (/ (:deduplicated stats) secs)))))
        (catch InterruptedException e))
      (if-not (get-in @view-system [:statistics :stop?])
        (recur)))))

(defn start-logger!
  "Starts a logger thread that will enable collection of view statistics
   which the logger will periodically write out to the log."
  [^Atom view-system log-interval]
  (trace "starting logger. logging at" log-interval "secs intervals")
  (if (get-in @view-system [:statistics :logger])
    (error "cannot start new logger thread until existing thread is stopped")
    (let [logger (Thread. ^Runnable (logger-thread view-system log-interval))]
      (swap! view-system update-in [:statistics] assoc
             :logger logger
             :stop? false)
      (reset-stats! view-system)
      (.start logger)))
  view-system)

(defn stop-logger!
  "Stops the logger thread."
  [^Atom view-system & [dont-wait-for-thread?]]
  (trace "stopping logger")
  (let [^Thread logger-thread (get-in @view-system [:statistics :logger])]
    (swap! view-system assoc-in [:statistics :stop?] true)
    (if logger-thread (.interrupt logger-thread))
    (if-not dont-wait-for-thread? (.join logger-thread))
    (swap! view-system assoc-in [:statistics :logger] nil))
  view-system)

(defn hint
  "Create a hint."
  [namespace hint type]
  {:namespace namespace :hint hint :type type})

(defn queue-hints!
  "Queues up hints in the view system so that they will be picked up by the refresh
   watcher and dispatched to the workers resulting in view updates being sent out
   for the relevant views/subscribers."
  [^Atom view-system hints]
  (trace "queueing hints" hints)
  (swap! view-system update-in [:hints] (fnil into #{}) hints)
  view-system)

(defn put-hints!
  "Adds a collection of hints to the view system by using the view system
   configuration's :put-hints-fn."
  [^Atom view-system hints]
  ((:put-hints-fn @view-system) view-system hints)
  view-system)

(defn- ->views-map
  [views]
  (map vector (map id views) views))

(defn add-views!
  "Add a collection of views to the system."
  [^Atom view-system views]
  (swap! view-system update-in [:views] (fnil into {}) (->views-map views))
  view-system)

(def default-options
  "Default options used to initialize the views system via init!"
  {
   ; *REQUIRED*
   ; a function that is used to send view refresh data to subscribers.
   ; this function must be set for normal operation of the views system.
   ; (fn [subscriber-key [view-sig view-data]] ...)
   :send-fn            nil

   ; *REQUIRED*
   ; a function that adds hints to the view system. this function will be used
   ; by other libraries that implement IView. this function must be set for
   ; normal operation of the views system. the default function provided
   ; will trigger relevant view refreshes immediately.
   ; (fn [^Atom view-system hints] ... )
   :put-hints-fn       (fn [^Atom view-system hints] (refresh-views! view-system hints))

   ; *REQUIRED*
   ; the size of the queue used to hold view refresh requests for
   ; the worker threads. for very heavy systems, this can be set
   ; higher if you start to get warnings about dropped refresh requests
   :refresh-queue-size 1000

   ; *REQUIRED*
   ; interval in milliseconds at which the refresh watcher thread will
   ; check for any queued up hints and dispatch relevant view refresh
   ; updates to the worker threads.
   :refresh-interval   1000

   ; *REQUIRED*
   ; the number of refresh worker threads that poll for view refresh
   ; requests and dispatch updated view data to subscribers.
   :worker-threads     8

   ; a list of IView instances. these are the views that can be subscribed
   ; to. views can also be added/replaced after system initialization through
   ; the use of add-views!
   :views              nil

   ; a function that authorizes view subscriptions. should return true if the
   ; subscription is authorized. if not set, no view subscriptions will require
   ; any authorization.
   ; (fn [view-sig subscriber-key context] ... )
   :auth-fn            nil

   ; a function that is called when subscription authorization fails.
   ; (fn [view-sig subscriber-key context] ... )
   :on-unauth-fn       nil

   ; a function that returns a namespace to use for view subscriptions
   ; (fn [view-sig subscriber-key context] ... )
   :namespace-fn       nil

   ; interval in milliseconds at which a logger will write view system
   ; statistics to the log. if not set, the logger will be disabled.
   :stats-log-interval nil
   })

(defn init!
  "Initializes the view system for use with the list of views provided.

   An existing atom that will be used to store the state of the views
   system can be provided, otherwise one will be created. Either way,
   the atom with the initialized view system is returned.

   options is a map of options to configure the view system with. See
   views.core/default-options for a description of the available options
   and the defaults that will be used for any options not provided in
   the call to init!."
  ([^Atom view-system options]
   (let [options (merge default-options options)]
     (trace "initializing views system using options:" options)
     (reset! view-system
             {:refresh-queue (ArrayBlockingQueue. (:refresh-queue-size options))
              :views         (into {} (->views-map (:views options)))
              :send-fn       (:send-fn options)
              :put-hints-fn  (:put-hints-fn options)
              :auth-fn       (:auth-fn options)
              :on-unauth-fn  (:on-unauth-fn options)
              :namespace-fn  (:namespace-fn options)
              ; keeping a copy of the options used during init allows other libraries
              ; that plugin/extend views functionality (e.g. IView implementations)
              ; to make use of any options themselves
              :options       options})
     (start-update-watcher! view-system (:refresh-interval options) (:worker-threads options))
     (when-let [stats-log-interval (:stats-log-interval options)]
       (swap! view-system assoc :logging? true)
       (start-logger! view-system stats-log-interval))
     view-system))
  ([options]
    (init! (atom {}) options)))

(defn shutdown!
  "Shuts the view system down, terminating all worker threads and clearing
   all view subscriptions and data."
  [^Atom view-system & [dont-wait-for-threads?]]
  (trace "shutting down views sytem")
  (stop-update-watcher! view-system dont-wait-for-threads?)
  (if (:logging? @view-system)
    (stop-logger! view-system dont-wait-for-threads?))
  (reset! view-system {})
  view-system)
