"""Reccommended movies.

Base version has the following additions (exercises in Lecture 31):

* discard bad ratings TODO
* implemented Pearson correlation, Jaccard coefficient, conditional probabilty TODO
* new similarity using number of co-raters TODO
* use genre info from u.items to boost scores from movies in same genre TODO
"""

import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

def makePairs((user, ratings)):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))

def filterDuplicates( (userID, ratings) ):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2

def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)

def computePearsonCorrelationCoefficient(ratingPairs):
    numPairs = 0
    if not ratingPairs:
        return (0, 0)

    muX = sum(1.*ratingX for (ratingX, _) in ratingPairs)/len(ratingPairs)
    muY = sum(1.*ratingY for (_, ratingY) in ratingPairs)/len(ratingPairs)

    cov = sum_sqdev_x = sum_sqdev_y = 0
    for ratingX, ratingY in ratingPairs:
        dev_x = ratingX - muX
        dev_y = ratingY - muY
        cov += dev_x * dev_y
        sum_sqdev_x += dev_x**2
        sum_sqdev_y += dev_y**2
        numPairs += 1

    numerator = cov
    denominator = sqrt(sum_sqdev_x) * sqrt(sum_sqdev_y)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)

def computeJaccardCoefficient(ratingPairs):
    """Compute Jaccard coefficient for ratings of pair of movies.

    See https://en.wikipedia.org/wiki/Jaccard_index. In what proportion of cases
    did the user rate both movies the same.
    """
    numPairs = numerator = denominator = 0
    for ratingX, ratingY in ratingPairs:
        if ratingX == ratingY:
            numerator += 1
            denominator += 1
        else:
            denominator += 1
        numPairs += 1

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)

#laptop doesn't like using both cores...
#conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
conf = SparkConf().setMaster("local").setAppName("MovieSimilarities")
sc = SparkContext(conf = conf)

print "\nLoading movie names..."
nameDict = loadMovieNames()

data = sc.textFile("file:///SparkCourse/ml-100k/u.data")

# Map ratings to key / value pairs: user ID => movie ID, rating
ratings = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

# Emit every movie rated together by the same user.
# Self-join to find every combination.
joinedRatings = ratings.join(ratings)

# At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

# Filter out duplicate pairs
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

# Now key by (movie1, movie2) pairs.
moviePairs = uniqueJoinedRatings.map(makePairs)

# We now have (movie1, movie2) => (rating1, rating2)
# Now collect all ratings for each movie pair and compute similarity
moviePairRatings = moviePairs.groupByKey()

# We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
# Can now compute similarities.
moviePairSimilarities = moviePairRatings.mapValues(computeJaccardCoefficient).cache()

# Save the results if desired
#moviePairSimilarities.sortByKey()
#moviePairSimilarities.saveAsTextFile("movie-sims")

# Extract similarities for the movie we care about that are "good".
if (len(sys.argv) > 1):

    scoreThreshold = 0.97
    coOccurenceThreshold = 50

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(lambda((pair,sim)): \
        (pair[0] == movieID or pair[1] == movieID) \
        and sim[0] > scoreThreshold and sim[1] > coOccurenceThreshold)

    # Sort by quality score.
    results = filteredResults.map(lambda((pair,sim)): (sim, pair)).sortByKey(ascending = False).take(10)

    print "Top 10 similar movies for " + nameDict[movieID]
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1])
