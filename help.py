from sklearn.cluster import KMeans
from pydataset import data

# some data to play with
tips = data('tips')

# which features are we clustering on?
cluster_columns = ['total_bill', 'tip']

# make the clusters
kmeans = KMeans(n_clusters=3).fit(tips[cluster_columns])

# add the clusters back to my original dataframe
tips['cluster'] = kmeans.predict(tips[cluster_columns])

# find the avg by cluster for the cluster_columns
cluster_means = tips.groupby('cluster')[cluster_columns].transform('mean')
# rename columns
cluster_means.columns = ['cluster_bill_avg', 'cluster_tip_avg']

# rejoin to the original dataframe
tips.join(cluster_means)