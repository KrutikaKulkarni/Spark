
# coding: utf-8

# This tutorial is to introduce you to the fudnamentals of Spark SQL.
# Quick summary of how Spark SQL works:
# a) It uses DataFrames, which are collections of data distributed across data nodes.
# b) Driver program typically launches the application. Executors, which run on data nodes,
#     are responsible for running the actual code.
# c) Results are typically returned to the driver.
# You will need a SQL context. You create one as follows:
# sqlContext = SQLContext(sc) where sc is the SparkContext.
# 
# 

# In[2]:

#let us see if a SQL Context exists
sqlContext


# In[3]:

#A DataFrame is like a relational table
#it consists of Row objects
#A row object can be created as follows
from pyspark.sql import Row
cricketer = Row(name='Viv Richards', age = 62, country='WI', batting_average=58.67)


# In[4]:

cricketer.name


# In[5]:

(cricketer.name, cricketer.age, cricketer.batting_average)


# In[6]:

cricketer['name'], cricketer['age'], cricketer['batting_average']


# In[7]:

#we can create a table of cricketers from a Python list of tuples
cricketers = [('Viv Richards', 62, "WI", 58.67), ('Greg Chappell', 64, "AUS", 56.83),              ('Doug Walters', 68, "AUS", 48.9), ('VVS Laxman', 41, "IND", 46.7),              ('Sachin Tendulkar', 42, 'IND', 57.8), ('Rahul Dravid', 41, 'IND', 54.5),              ('Garry Sobers', 78, "WI", 58.9), ('Rohan Kanhai', 75, "WI", 51.3),              ('GR Vishwanath', 68, "IND", 44.3)]
cricketers_df = sqlContext.createDataFrame(cricketers, ['Name','Age','Country','Average'])


# In[8]:

#use show() to display records; show(n) will display n records
cricketers_df.show()


# In[9]:

#use filter to limit records - example shows cricketers from WI (West Indies)
cricketers_df.filter(cricketers_df.Country == "WI").show()


# In[10]:

#let us look at the schema of our dataframe
cricketers_df.printSchema()


# In[13]:

#Display cricketers who are over 70
cricketers_df.where(cricketers_df.Age > 70).show()


# In[14]:

#let us create another dataframe for Country Abbreviations and Country Names
countries = [("IND", "INDIA"), ("WI", "WEST INDIES"), ("AUS", "AUSTRALIA"),              ("ENG", "ENGLAND"), ("NZ", "NEW ZEALAND")]
countries_df = sqlContext.createDataFrame(countries, ["ID","NAME"])
countries_df.show()


# In[15]:

#let us try out different join operations
outer_join_df = cricketers_df.join(countries_df, cricketers_df.Country == countries_df.ID, 'outer')
outer_join_df.show()


# In[16]:

inner_join_df = cricketers_df.join(countries_df, cricketers_df.Country == countries_df.ID, 'inner')
inner_join_df.show()


# In[17]:

left_outer_join_df = cricketers_df.join(countries_df, cricketers_df.Country == countries_df.ID, 'left_outer')
left_outer_join_df.show()


# In[18]:

right_outer_join_df = cricketers_df.join(countries_df, cricketers_df.Country == countries_df.ID, 'right_outer')
right_outer_join_df.show()


# In[21]:

inner_join_df = cricketers_df.join(countries_df, cricketers_df.Country == countries_df.ID, 'inner')                .select(cricketers_df.Name, countries_df.NAME.alias("Country_Name"), 'Average').show()


# In[22]:

#reading from a text file
text_df = sqlContext.read.text("/Users/snerur/sample.txt")
text_df.show()


# In[43]:

#let us get the words into a column
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, explode
my_func = udf(lambda s: s.lower().split(), ArrayType(StringType()))
words_df = text_df.select(my_func(text_df.value).alias('words'))
#words_df.show()
#we need to "explode" the list of words so that we have one word per row
exploded_df = words_df.select(explode(words_df.words).alias("words"))
exploded_df.show()


# In[53]:

#get the count for each word, sorted in descending order
exploded_df.groupBy('words').count().sort("count", ascending=False).show()


# In[ ]:



