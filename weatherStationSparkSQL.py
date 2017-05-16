
# coding: utf-8

# In[2]:

#Let us first read in the weather data and create an RDD
rdd = sc.textFile("/Users/kruti/sparkExamples/2015.csv")


# In[2]:

#Howm many records does the file have?
rdd.count()


# In[3]:

#Read in weather station data
#Here is one way to get the average precipitation of all weather stations
rdd_prcp = rdd.map(lambda x: x.split(",")).filter(lambda x: x[2] == "PRCP")
avg = rdd_prcp.map(lambda x: (1,float(x[3]))).groupByKey().mapValues(list).map(lambda x: sum(x[1])/len(x[1]))
avg.first()



# In[4]:

#perhaps an easier way....
counts = rdd_prcp.count() #number of records that have precipitation
total = rdd_prcp.map(lambda x: float(x[3])).reduce(lambda x, y: x+ y)
total /counts


# In[5]:

#how many unique weather stations does the data set have
weather_stations = rdd.map(lambda x: x.split(",")).map(lambda x: x[0]).distinct().count()
weather_stations


# In[18]:

#4	How many records are associated with the weather station “US1FLSL0019”?
rdd_parsed = rdd.map(lambda x: x.split(","))
records = rdd_parsed.filter(lambda x: x[0] == "US1FLSL0019" )
counts = records.count()
counts


# In[19]:

#5.	What is the average precipitation of weather station “US1FLSL0019”?
total = records.filter(lambda x: x[2] == 'PRCP').map(lambda x: float(x[3])).reduce(lambda x, y: x + y)
print "Average Precipitation for USIFLSL0019: %.2f" % (total / counts)


# In[1]:

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()


# In[5]:

from datetime import datetime, date
def parse(record):
    fields = record.split(",")
    station = fields[0]
    year = int(fields[1][:4])
    month = int(fields[1][4:6])
    day = int(fields[1][6:])
    event_date = datetime(year, month, day).date()
    metric = fields[2]
    value = float(fields[3])
    return (station, event_date, metric, value)
#parse("US100,20050101,PRCP,173")
rdd_parsed = rdd.map(parse)
rdd_parsed.take(3)


# In[6]:

df = spark.createDataFrame(rdd_parsed, ['STATION','DATE','METRIC','VALUE'])
df.show(3)


# In[7]:

#practice a few dataframe commands
df.select(df['STATION'], df['METRIC']).show(5)
df.filter(df['METRIC'] == 'PRCP').select(df['STATION'], df['VALUE']).show(5)


# In[8]:

#register a table to facilitate SQL queries
df.createOrReplaceTempView("weather") #note that registerTempTable has been deprecated in Spark 2.X
result = spark.sql("select count(*) from weather")
result.show()


# In[15]:

#Average precipitation by weather station
result = spark.sql("select STATION, AVG(VALUE) AS AVERAGE from weather where METRIC = 'PRCP' GROUP BY STATION ORDER BY AVERAGE DESC")
result.show(10)


# In[20]:

stations = spark.sql("select distinct(STATION) from weather")
stations.count()

