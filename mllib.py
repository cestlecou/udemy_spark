from pyspark import SparkConf, SparkContext
from pyspark.mllib.clustering import KMeans

import numpy as np
from operator import add

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

data = np.array([0.0,0.0, 1.0,1.0, 9.0,8.0, 8.0,9.0]).reshape(4, 2)

model = KMeans.train(sc.parallelize(data),
                     2,
                     maxIterations=10,
                     runs=30,
                     initializationMode="random",
                     seed=50,
                     initializationSteps=5,
                     epsilon=1e-4)

#silhouette score spark
labels = model.predict(sc.parallelize(data))
with_labels = sc.parallelize(data).zip(labels)
errors_in_clusters = with_labels.map(lambda (x, cluster_index): (cluster_index, (x - model.clusterCenters[cluster_index])**2))

cluster_counts = with_labels.map(lambda (x,y): (y,x)).countByValue()
errors_in_clusters.reduceByKey(add)
zipByKey
divide, done

mse_array = with_cluster_centres.map(lambda (x, y): (x-y)**2).reduce(add)
mse = np.sum(mse_array)/(data.shape[0]*data.shape[1])

if __name__ == '__main__':
    for k in (2,3,4):
        model = KMeans.train(sc.parallelize(data),
                             k,
                             maxIterations=10,
                             runs=30,
                             initializationMode="random",
                             seed=50,
                             initializationSteps=5,
                             epsilon=1e-4)

        labels = model.predict(sc.parallelize(data))
        with_labels = sc.parallelize(data).zip(labels)
        with_cluster_centres = with_labels.map(lambda (x, cluster_index): (x, model.clusterCenters[cluster_index]))
        mse_array = with_cluster_centres.map(lambda (x, y): (x-y)**2).reduce(add)
        mse = np.sum(mse_array)/(data.shape[0]*data.shape[1])
        print 'k={}'.format(k), mse
