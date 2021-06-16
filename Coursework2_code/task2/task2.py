# We import the requested libraries:
from pyspark import SparkContext
from operator import add

# SparkContext is the entry point to any spark functionality, to start the driver program we have to 
# initialise a SparkContext object. We need to create SparkContext only in programs, pyspark shell does 
# this automatically. In the following line Master is 'local' (URL of the cluster it connects to), and 
# appName is 'pyspark' (name of our job):
sc = SparkContext('local', 'pyspark')

# This function, age_group, returns a string with the age range given a certain age:
def age_group(age):
	
	if age < 10:
		return '0-10'
	elif age < 20:
		return '10-20'
	elif age < 30:
		return '20-30'
	elif age < 40:
		return '30-40'
	elif age < 50:
		return '40-50'
	elif age < 60:
		return '50-60'
	elif age < 70:
		return '60-70'
	elif age < 80:
		return '70-80'
	else:
		return '+80'

# This function, parse_with_age_group, gets the different values of u.user (user id, age, gender, 
# occupation and zipcode) and returns different variables (userid, age_group, gender, occupation,
# zip and age):
def parse_with_age_group(data):

	userid, age, gender, occupation, zip = data.split("|") # creates a list with the different attributes.	
	return userid, age_group(int(age)), gender, occupation, zip, int(age) # returns the attributes in this order.


# WRITE YOUR CODE HERE

# We will proceed in the following manner:
	# 1. Read and parse data.
	# 2. Create an RDD for each age_group ('40-50' and '50-60').
	# 3. Obtain the top-10 occupations of each group ('40-50' and '50-60').
	# 4. Create two new RDDs with this new information.
	# 5. Intersect the two previous top-10 occupations per age group to obtain the common occupations.

# First of all, we will create an RDD (Resilient Distributed Datasets), which enables efficient data
# reuse. An RDD is a data structure that serves as the core unit of data for Apache Spark. From the 
# user's side, he can manipulate it, control its partioning, or make it persistent in memory, which
# avoids the need of writing data in disk.

# We populate the RDD with the built-in function 'sc.textFile', which creates an RDD based on a .txt
# file:
data = sc.textFile("file:///home/cloudera/Desktop/cw2/task2/u.user.txt") # as parameter you specify the path to the .txt file.

# Now we convert our RDD in a new one with the age_group in it:
users = data.map(parse_with_age_group)

# Next, we create a group for users in their forties and other with users in their fifties, we do so by 
# using .filter(): 
forties = users.filter(lambda x: '40-50' in x) # each entry in 'users' is a list, so, if '40-50' or '50-60' is an
fifties = users.filter(lambda x: '50-60' in x) # element of that list, it creates a new RDD with those values.

# Now we have to compute their frequencies to get the most common occupations. The function .countByValue()
# returns a dictionary with the form KEY ('occupation')-VALUE (frequency). We only care about the 4th position, 
# its occupation, that is why we map only that value.
occ_freqs_forties = forties.map(lambda x: x[3]).countByValue() 
occ_freqs_fifties = fifties.map(lambda x: x[3]).countByValue()

# Now, we create two new RDDs with the top-10 most frequent occupations in each age group. We will benefit from
# the 'sorted' function in Python. We will sort in descending order (reverse=True) the dictionary created in
# the previous step and get only the keys. After that, we select the 10 first elements:
top_10_forties = sc.parallelize(sorted(occ_freqs_forties, key = occ_freqs_forties.get, reverse = True)[:10])
top_10_fifties = sc.parallelize(sorted(occ_freqs_fifties, key = occ_freqs_fifties.get, reverse = True)[:10])

# Once we have two RDDs, we only need their intersection, as the question description asks us to output all 
# occupations that are performed by these two groups and are among the 10 most frequent occupations for the 
# users in each group. This can be done easily with the function .intersection()
common_top10_occs = top_10_forties.intersection(top_10_fifties)

# Lastly, we print the final results. 
print common_top10_occs.collect()
