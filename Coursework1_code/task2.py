# In this Python file we will write the code necessary for task 2 of the Coursework 1.
# Great part of the inspiration comes from https://github.com/mattwg/mrjob-examples/blob/master/most_used_words.py
# This github repository shows some applications and examples of mrjob

# We have to output the 10 most frequent values in the Age attribute. This values are in the first column of adult.data.

# We will start by importing the libraries we will need.
from mrjob.job import MRJob # this module let us write MapReduce jobs in Python 2.6+/3.3+ and run them on several platforms.
from mrjob.step import MRStep # this module allows us to set the steps our program is going to make. Which mapper/combiner/reducer goes before or after
import re # this one is for Regular expressions (RegEx) in Python. 

WORD_RE = re.compile(r"[\w']+") # this command is to read the document/file we are wishing to apply the MapReduce task on.

# Now we create the class that will do all the job we want.
class agecount(MRJob):

	# We will define first the mapper. This mapper will accept as parameters an empty key (_) and a line as values. It will
	# output a number of key-value pairs (age, 1)
	def mapper(self, _, line):

		# This command will split the lines of adult.data and store them in a list called 'data'.
		data = line.split(", ") # Now, the age will be stored in data[0], as it was in the first column
		
		age = data[0].strip() # We store the age value of data in the variable 'age'
		# .strip() just removes spaces at the beginning and at the end of the string
		
		# 'yield' is similar to 'return', but it does not terminate the function. It produces key-value pairs.
		# In this case, we will yield each age in each line
		yield age, 1

	# Here we will define a combiner. A combiner takes a key and a subset of the values for that key as inputs and
	# returns zero or more (key, value) pairs. Combiners are optimisations that run immediately after each mapper and
	# can be used to decrease total data transfer. This is set mainly for optimization, it makes the MapReduce job
	# much faster.
	# https://mrjob.readthedocs.io/en/latest/guides/concepts.html#:~:text=A%20combiner%20takes%20a%20key,to%20decrease%20total%20data%20transfer.
	def combiner(self, age, count_of_ages):
		
		# We yield the age as a key and the sum of the values of each age as the value
		yield age, sum(count_of_ages)

	
	# Now we define the first reducer. This reducer accepts as parameters the key (age) and value (count_of_ages) from the 
	# mapper and combiner defined above, and yields (returns) the sum of the keys we have passed. In other words, it yields 
	# the number of times each age appears.
	def reducer1(self, age, count_of_ages):

		# We yield the sum of the values of each age. This time we will not return anu key. Now we will return a value that
		# is actually a pair of values, the number of appearances of each age and the actual age value.
		yield None, (sum(count_of_ages), age)

	# Now we define the second reducer, the one that will yield/output the 10 most frequent values of 'age'. This reducer
	# will accept as parameters an empty key (_) and the value from the reducer1, a tuple (sum_of_ages, age_value), and it will output
	# the 10 most frequent age values alongside with the actual age value.
	def reducer2(self, _, sum_of_ages_pairs):

		# One of the characteristics of a generator is that once exhausted, you cannot reuse it
		# https://stackoverflow.com/questions/46991616/yield-both-max-and-min-in-a-single-mapreduce

		# So we will create a list instead of a generator, but this list will have the same content as the generator we 
		# have passed as a parameter. As a tuple cannot be sorted, we are basically converting it into a list so it can 
		# be sorted.
		sum_of_ages_pairs = list(sum_of_ages_pairs)# we create the list of the generator
		sum_of_ages_pairs.sort(reverse=True) # we sort the list of values from the most frequent to the least frequent this is done
						     # with the parameter reverse=True

		count = 0 # we initialise this variable to 0 so it serves us as a counter, so we can later exit the for loop when 
			  # we have already outputted the 10 most frequent values. 

		# We create a for loop so it iterates through the 10 first values of the sorted list we created above
		for age in sum_of_ages_pairs: # iterate through all values in our list
			if count == 10: # Here you can change the value for the top-N most frequent values. Currently, N=10.
				break # whenever our counter reaches 10, we exit the loop as we already outputted what we wanted
			yield age # here we yield/return/output one value per iteration
			count += 1 # increase the counter after outputting the top-10 values

	# Lastly, we set the steps we want our program to follow. mrjob by defalut uses a single MapReduce task to execute the program.
	# We redefine steps() to create two jobs, each defined with MRStep(). This is mainly the order in which we will execute the 
	# map/reduce jobs.
	def steps(self):
		return [
			MRStep(mapper = self.mapper, # first, we map the different age values in the specified document (adult.data)
			       combiner = self.combiner, # second, we call our combiner to optimise the time of execution
			       reducer = self.reducer1), # third, we reduce the different ages and return the count of each one
			MRStep(reducer = self.reducer2) # fourth and last, we return the 10 most frequent age values
			]

# You can understand better what this last part of the code does in the following StackOverflow entry:
# https://stackoverflow.com/questions/419163/what-does-if-name-main-do
if __name__ == "__main__":
	agecount.run()


