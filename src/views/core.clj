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
;;
;;  :hashes {view-sig hash, ...}
;;  :subscribed {subscriber-key #{view-sig, ...}}
;;  :subscribers {view-sig #{subscriber-key, ...}}
;;  :hints #{hint1 hint2 ...}
;;
;;  }
;;
;;  Each hint has the form {:namespace x :hint y}

(def refresh-queue-size
  (if-let [n (:views-refresh-queue-size env)]
    (Long/parseLong n)
    1000))

(def statistics (atom {}))

(defn reset-stats!
  []
  (swap! statistics (fn [s] {:enabled (boolean (:enabled s)), :refreshes 0, :dropped 0, :deduplicated 0})))

(defn collect-stats? [] (:enabled @statistics))

(reset-stats!)


(def refresh-queue (ArrayBlockingQueue. refresh-queue-size))

(defn subscribe-view!
  [view-system view-sig subscriber-key]
  (-> view-system
      (update-in [:subscribed subscriber-key] (fnil conj #{}) view-sig)
      (update-in [:subscribers view-sig] (fnil conj #{}) subscriber-key)))

(defn update-hash!
  [view-system view-sig data-hash]
  (update-in view-system [:hashes view-sig] #(or % data-hash))) ;; see note #1 in NOTES.md

(defn subscribe!
  [view-system namespace view-id parameters subscriber-key]
  (when-let [view (get-in @view-system [:views view-id])]
    (let [view-sig [namespace view-id parameters]]
      (swap! view-system subscribe-view! view-sig subscriber-key)
      (future
        (try
          (let [vdata     (data view namespace parameters)
                data-hash (hash vdata)]
            ;; Check to make sure that we are still subscribed. It's possible that
            ;; an unsubscription event came in while computing the view.
            (when (contains? (get-in @view-system [:subscribed subscriber-key]) view-sig)
              (swap! view-system update-hash! view-sig data-hash)
              ((get @view-system :send-fn) subscriber-key [[view-id parameters] vdata])))
          (catch Exception e
            (error "error subscribing:" namespace view-id parameters
                   "e:" e "msg:" (.getMessage e))))))))

(defn remove-from-subscribers
  [view-system view-sig subscriber-key]
  (update-in view-system [:subscribers view-sig] disj subscriber-key))

(defn unsubscribe!
  [view-system namespace view-id parameters subscriber-key]
  (swap! view-system
         (fn [vs]
           (-> vs
               (update-in [:subscribed subscriber-key] disj [namespace view-id parameters])
               (remove-from-subscribers [namespace view-id parameters] subscriber-key)))))

(defn unsubscribe-all!
  "Remove all subscriptions by a given subscriber."
  [view-system subscriber-key]
  (swap! view-system
         (fn [vs]
           (let [view-sigs (get-in vs [:subscribed subscriber-key])
                 vs*       (update-in vs [:subscribed] dissoc subscriber-key)]
             (reduce #(remove-from-subscribers %1 %2 subscriber-key) vs* view-sigs)))))

(defn refresh-view!
  "We refresh a view if it is relevant and its data hash has changed."
  [view-system hints [namespace view-id parameters :as view-sig]]
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
  [view-system]
  (reduce into #{} (vals (:subscribed view-system))))

(defn active-view-count
  "Returns a count of views with at least one subscriber."
  [view-system]
  (count (remove #(empty? (val %)) (:subscribers view-system))))

(defn pop-hints!
  "Return hints and clear hint set atomicly."
  [view-system]
  (let [p (swap-pair! view-system assoc :hints #{})]
    (or (:hints (first p)) #{})))

(defn refresh-views!
  "Given a collection of hints, or a single hint, find all dirty views and schedule them for a refresh."
  ([view-system hints]
   (debug "refresh hints:" hints)
   (mapv #(refresh-view! view-system hints %) (subscribed-views @view-system))
   (swap! view-system assoc :last-update (System/currentTimeMillis)))
  ([view-system]
   (refresh-views! view-system (pop-hints! view-system))))

(defn can-refresh?
  [last-update min-refresh-interval]
  (> (- (System/currentTimeMillis) last-update) min-refresh-interval))

(defn wait
  [last-update min-refresh-interval]
  (Thread/sleep (max 0 (- min-refresh-interval (- (System/currentTimeMillis) last-update)))))

(defn refresh-worker-thread
  "Handles refresh requests."
  [view-system]
  (fn []
    (try
      (when-let [[namespace view-id parameters :as view-sig] (.poll ^ArrayBlockingQueue refresh-queue 60 TimeUnit/SECONDS)]
        (when (collect-stats?) (swap! statistics update-in [:refreshes] inc))
        (try
          (let [view  (get-in @view-system [:views view-id])
                vdata (data view namespace parameters)
                hdata (hash vdata)]
            (when-not (= hdata (get-in @view-system [:hashes view-sig]))
              (doseq [s (get-in @view-system [:subscribers view-sig])]
                ((:send-fn @view-system) s [[view-id parameters] vdata]))
              (swap! view-system assoc-in [:hashes view-sig] hdata)))
          (catch Exception e
            (error "error refreshing:" namespace view-id parameters
                   "e:" e "msg:" (.getMessage e)))))
      (catch InterruptedException e))
    (if-not (:stop-workers? @view-system)
      (recur)
      (debug "exiting worker thread"))))

(defn refresh-watcher-thread
  [view-system min-refresh-interval]
  (fn []
    (let [last-update (:last-update @view-system)]
      (try
        (if (can-refresh? last-update min-refresh-interval)
          (refresh-views! view-system)
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
  [view-system min-refresh-interval threads]
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
        (.start t)))))

(defn stop-update-watcher!
  "Stops threads for the views refresh watcher and worker threads."
  [view-system]
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
  [view-system msecs]
  (swap! statistics assoc-in [:enabled] true)
  (let [secs (/ msecs 1000)]
    (.start (Thread. (fn []
                       (Thread/sleep msecs)
                       (let [stats @statistics]
                         (reset-stats!)
                         (info "subscribed views:" (active-view-count @view-system)
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
  [view-system hint]
  (swap! view-system update-in [:hints] (fnil conj #{}) hint))

(defn add-views!
  "Add a collection of views to the system."
  [view-system views]
  (swap! view-system update-in [:views] (fnil into {}) (map vector (map id views) views)))

(comment
  (defrecord SQLView [id query-fn]
    IView
    (id [_] id)
    (data [_ namespace parameters]
      (j/query (db/firm-connection namespace) (hsql/format (apply query-fn parameters))))
    (relevant? [_ namespace parameters hints]
      (let [tables (query-tables (apply query-fn parameters))]
        (boolean (some #(not-empty (intersection % talbes)) hints)))))

  (def memory-system (atom {}))

  (reset! memory-system {:a {:foo 1 :bar 200 :baz [1 2 3]}
                         :b {:foo 2 :bar 300 :baz [2 3 4]}})

  (defrecord MemoryView [id ks]
    IView
    (id [_] id)
    (data [_ namespace parameters]
      (get-in @memory-system (-> [namespace] (into ks) (into parameters))))
    (relevant? [_ namespace parameters hints]
      (some #(and (= namespace (:namespace %)) (= ks (:hint %))) hints)))

  (def view-system
    (atom
      {:views   {:foo (MemoryView. :foo [:foo])
                 :bar (MemoryView. :bar [:bar])
                 :baz (MemoryView. :baz [:baz])}
       :send-fn (fn [subscriber-key data] (println "sending to:" subscriber-key "data:" data))}))

  (subscribe! view-system :a :foo [] 1)
  (subscribe! view-system :b :foo [] 2)
  (subscribe! view-system :b :baz [] 2)

  (subscribed-views @view-system)

  (doto view-system
    (add-hint! [:foo])
    (add-hint! [:baz]))


  (refresh-views! view-system)

  ;; Example of function that updates and hints the view system.
  (defn massoc-in!
    [memory-system namespace ks v]
    (let [ms (swap! memory-system assoc-in (into [namespace] ks) v)]
      (add-hint! view-system ks)
      ms))

  (massoc-in! memory-system :a [:foo] 1)
  (massoc-in! memory-system :b [:baz] [2 4 3])


  (start-update-watcher! view-system 1000)

  )
