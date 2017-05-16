# Spark

# Inverted Index
A Spark program that creates an inverted list of words and the files that contain them. That is, for each word, you will display a list of files that contain that word. A sample output is given below:
‘anger’		[‘histories’, ‘tragedies’]
‘laugh’		[‘comedies’, ‘poems’, ‘histories’]

# Inverted index with word count
Modification to the first program to display the number of times the word occurs in each file. A sample output is shown below:
‘anger’		{‘histories’: 3, ‘tragedies’: 8}
‘laugh’		{‘comedies’: 7, ‘poems’: 2, ‘histories’: 15}

# word count for abstact title word for each author 
The data contains entries that look like:
journals/cl/SantoNR90:::Michele Di Santo::Libero Nigro::Wilma Russo:::Programmer-Defined Control Abstractions in Modula-2.
that represent bibliographic information about publications, formatted as follows:
paper-id:::author1::author2::…. ::authorN:::title

This program computes how many times every term occurs across titles, for each author.
For example, the author Alberto Pettorossi has the following terms occur in titles with the indicated cumulative frequencies (across all his papers): program:3, transformation:2, transforming:2, using:2, programs:2, and logic:2. 

All the above programs are done using Spark RDDs. 
