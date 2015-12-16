Code for udemy Apache spark course.

Interesting scripts:

* popularmoviesnicer.py - use of broadcast variables to allow worker nodes to
link movie ids with movie names.
* degrees-of-separation.py - breadth-first search implementation, also the use
of accumulators.
* moviesimilarities.py - "local[*]" to use all cores on computer

Tricks:

* Flip key value pair to use sortByKey on rdd.
