(ns ^{:doc "An connection wrapper for existing graphs with memory-only updates"
      :author "Paula Gearon"}
    asami.durable.memwrap
    (:require [asami.graph :as gr :refer [Graph graph-add graph-delete graph-diff resolve-triple count-triple]]
              [asami.internal :refer [now instant? to-timestamp]]
              [asami.storage :as storage]
              [asami.wrapgraph :as wrapgraph]
              [asami.memory :refer [->MemoryWrapConnection ->MemoryConnection]]
              [schema.core :as s :include-macros true]))

(defn as-of*
  "A generalized implementation of as-of/since that offloads the work to the wrapped database.
  If the requested database is more recent than what the wrapped database provides, then return
  the current database."
  [this wrapped timestamp t-val >cmp]
  (let [the-db (as-of* wrapped t-val)]
    (if (and the-db (identical? the-db wrapped))
      (cond
        (instant? t-val) (let [ts (to-timestamp t)]
                           ;; if the time is after the end of the root database, then return this hybrid
                           (if (>cmp (compare ts timestamp) 0)
                             this
                             wrapped))

        (int? t-val) (if (>cmp t-val (storage/last-tx this))
                       this
                       wrapped))
      the-db)))

(defrecord HybridDatabase [wgraph wrapped timestamp]
  storage/Database
  (last-timestamp [this] timestamp)
  (last-tx [this] (inc (storage/last-tx wrapped)))
  (as-of [this t-val] (as-of* this wrapped timestamp t-val >))
  (as-of-t [this] (storage/last-tx this))
  (as-of-time [this] timestamp)
  (since [this t] (as-of* this wrapped timestamp t-val >=))
  (since-t [this] (storage/last-tx this))
  (graph [this] wgraph)
  (entity [this id nested?] (reader/ident->entity wgraph id nested?)))

(defn wrapped-db
  "Creates a DB based on a wrapgraph"
  [db]
  (->HybridDatabase (wrapgraph/wrap-graph (storage/graph db)) db (now)))

(defn wrap-connection
  "Returns a new wrapped connection. This is almost the same as a normal
  in-memory database, but the connection is no longer attached to the session."
  [connection]
  ;; create a wrapped with a copy of the state
  (let [nm (storage/get-name connection)
        db (wrapped-db (storage/db connection))
        internal-connection (->MemoryConnection nm (atom {:db db :history []}))]
    (->MemoryWrapConnection internal-connection
                            nm
                            (storage/next-tx connection)
                            wrapped-db)))
