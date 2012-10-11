(ns djdb.core
  (:use [djdb.mongo]
        [clojure.pprint :only [pp pprint]]))

(defn print-map
  [map]
  (pprint (dissoc map :object :_id)))

(defn print-maps
  [maps]
  (pprint (map #(dissoc % :object :_id) maps)))

(defn make-transition
  "Makes a new transition and adds it to the database,
   with given vectors as field values.
   Example from-vec: ['WTF' 13 16]
   Example to-vec: ['Raining' 1 4]
   Example tags-vec: [9 'easy' 'sudden']"
  [from-vec to-vec tags-vec]

  ;;[[from-song & from-cues]
  ;; [to-song & to-cues] tags-vec]
  ;; first check to see if anything is already there with that title
  ;; TODO
;;  (let [from-song (seq (search-songs {:title from-song}))
;;        to-song (seq (search-songs {:title to-song}))]
;;    )
  
  (add {:object "transition"
        :from from-vec
        :to to-vec
        :tags tags-vec}))

(defn make-mesh
  "Makes a new mesh and adds it to the database, with given vectors as field values."
  ; TODO
  [song1 song2 tags-vec]
  (add {:object "mesh"
        :song1 song1
        :song2 song2
        :tags tags-vec}))

(defn make-playset
  "Makes a playset from the given info, adds it to the database, then returns the new
   playset."
  [name songs & tags]
  (add {:object "playset"
        :name name
        :songs songs
        :tags (into [] tags)}))

(defn _make-song
  [criteria]
  (add (merge {:object "song"
               :title ""
               :artist ""
               :mod ""}
              criteria)))

(defn make-song
  ([title]
     (_make-song {:title title}))
  ([title artist]
     (_make-song {:title title
                 :artist artist}))
  ([title artist mod]
     (_make-song {:title title
                 :artist artist
                 :mod mod})))

(defn _search-object
  "Searches all of given object type for given criteria-map."
  [object criteria]
  (find-maps { "$and"
               [ { :object object }
                 criteria ] } ))

(defn search-transitions
  "Searches all transition objects for given criteria-map."
  [criteria]
  (_search-object "transition" criteria))

(defn search-meshes
  "Searches all mesh objects for given criteria-map."
  [criteria]
  (_search-object "mesh" criteria))

(defn search-songs
  "Searches all song objects for given criteria-map."
  [criteria]
  (_search-object "song" criteria))

(defn search-playsets
  "Searches all playset object for given criteria-map."
  [criteria]
  (find-maps "playset" criteria))

(defn make-regex-map
  [string]
  {"$regex" string "$options" "i"})

(defn print-transition-songs
  [transitions direction]
  (doall (map #(println (direction %)) transitions)))

(defn print-direction-header
  [direction title]
  (println (clojure.string/capitalize
            (clojure.string/replace direction #":" ""))
           (clojure.string/capitalize title)))

(defn invert [direction]
  (if (= :to direction)
    :from
    :to))

(defn get-transitions
  "Returns a list of all transitions involving given song title,
   can specify direction :to or :from as a keyword."
  ([title] (concat (get-transitions :from title)
                   (get-transitions :to title)))
  ([direction title]
     (let [transitions (seq (search-transitions {direction (make-regex-map title)}))]
       (if transitions
         (do (print-direction-header direction title)
             (print-transition-songs transitions (invert direction))
             transitions)))))

(defn get-meshes
  "Returns a list of all meshes involving given song title."
  [title]
  (let [meshes (search-meshes {"$or" [ {:song1 title}, {:song2 title} ] })]
    (do (println "Matching Meshes:")
        (print-maps meshes)
        meshes)))

(defn get-playsets
  "Returns a list of all playsets involving given song title."
  [title]
  (search-playsets {:songs (make-regex-map title)}))

(defn remove-fields
  "Removes given fields from given objects.
   Returns the updated sample or list."
  [objects & fields]
  (last (map #(edit objects {"$unset" {% 1}}) fields)))

(defn add-to-field
  "Updates given sample(s)'s given field's array in given playlist to also
   contain given input. Not destructive.
   Returns updated sample or list."
  [objects field & input]
  (edit objects {"$pushAll" {field (vec (flatten input))}}))

(defn add-to-playset
  "Adds given song-title to given playset"
  [playset title]
  (add-to-field playset :songs title))

(defn remove-from-field
  "Updates given sample(s)'s given field's array in given playlist to no longer
   contain any of given input. Returns the updated sample or list."
  [objects field & input]
  (edit objects {"$pullAll" {field (vec (flatten input))}}))

(defn remove-from-playset
  "Removes given song-title from given playset"
  [playset title]
  (remove-from-field playset :songs title))

(defn set-field
  "Updates given sample(s)'s fields in given playlist with given criteria data.
   Returns the updated sample or list. Destructive to old field data."
  [objects criteria]
  (edit objects {"$set" criteria}))

(defn tag
  "Adds given tag(s) to given objects. Returns a list of the updated
   sample(s). Note: duplicate tags are skipped."
  [objects & tags]
  (last (map #(edit objects {"$addToSet" {:tags %}}) tags)))

(defn untag
  "Removes given tag(s) from given objects. Returns the updated sample or list."
  [objects & tags]
  (remove-from-field objects :tags tags))

(defn find-tag
  "Returns a list of all objects with any of the given tag(s), or nil."
  [& tags]
  (find-maps {:tags {"$in" (vec tags)}}))

(defn find-not-tag
  "Returns a list of objects without any of the given tag(s), or nil."
  [& tags]
  (find-maps {:tags {"$nin" (vec tags)}}))
