# In this Python file we will write the code necessary for task 3 of the Coursework 1.

# We have to create something called 'Inverted Index' that serves mainly to find specific things faster. In our case we will 
# find symbols in a specific line row. Each row has a number of symbols, and we will have to output the symbols and the lines
# where these symbols are located. 

# We will start by importing the libraries we will need.
from mrjob.job import MRJob # this module let us write MapReduce jobs in Python 2.6+/3.3+ and run them on several platforms.
import re # this one is for Regular expressions (RegEx) in Python. 

WORD_RE = re.compile(r"[\w']+") # this command is to read the document/file we are wishing to apply the MapReduce task on.

# Now we create the class that will do all the job we want.
class inverseIndex(MRJob):

	# We will define first the mapper. This mapper will accept as parameters an empty key (_) and a line as values. It will
	# output a number of key-value pairs (symbol, line_number)
	def mapper(self, _, line):

		# This command will split the lines of msnbc_lines and store them in a list called 'data'.
		data = re.split(' +', line) # Now, each line will be stored in data. Depending on the line number, this data list
					    # will have one format or another. If the dataline has 6 integers, in other words, if
					    # the dataline is between 100000 and 989818 (last entry of the datafile), the first 
					    # value of the list will be the line number. In every other case, the first value of 
					    # data will be an empty string (''), so we would have to select the next value.

		# Here we store in line_number the value of the line where the information is.
		if data[0] == '': # If the line number is below 100000.
			del data[0] # delete the first element of the list.
		line_number = int(data.pop(0).strip()) # We store the line number (first value of data) in the variable 'line_number'.
		# .strip() just removes spaces at the beginning and at the end of the string.

		# 'yield' is similar to 'return', but it does not terminate the function. It produces key-value pairs.
		# In this case, we will yield a symbol and the line where it is located.
		for symbol in data: # For every symbol stored in the specific line number:
			yield symbol, line_number # yield/return/output the symbol and the line number of that specific symbol.
	
	# Next, we will define the reducer. This will collect all the symbols with the line number it belongs to and output a
	# sorted list of the line number where it appears. So, it will accept two different parameters: a key (symbol) and a 
	# value (the line number where it is located).	
	def reducer(self, symbol, line_number):
		
		# We create a set for one main reason: its characteristic related to duplicated values. A set cannot have two equal
		# values, if you try to add a value that is already stored there, it will omit it. This makes sets much more efficient.
		total_lines = set()

		# We traverse the list of values passed by the mapper and add the line numbers to the set created above.
		for line in line_number:
			total_lines.add(line) # For sets you use .add() instead of .append()

		# Now we convert the set into a list ans sort it storing the first rows at the beginning, just like the ouput shown in
		# the coursework description.
		total_lines = list(total_lines) # convert the set into a list
		total_lines.sort() # sort the values of the list converted in the last code line.

		# Now we are in position to yield/return the symbol and all the lines where that symbol appears. As we created a set, the
		# values of total_lines will not include any duplicated values.
		yield symbol, total_lines	


# You can understand better what this last part of the code does in the following StackOverflow entry:
# https://stackoverflow.com/questions/419163/what-does-if-name-main-do
if __name__ == "__main__":
	inverseIndex.run()
