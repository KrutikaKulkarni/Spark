# -*- coding: utf-8 -*-
"""
Created on Sat Apr 01 13:56:24 2017

@author: KrutikaKulkarni
"""
import string

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[*]").setAppName("InvertedIndex")
sc = SparkContext(conf = conf)
 
file1 = sc.wholeTextFiles("C:\Users\Krutika\Desktop\Shakespeare\*")
file2 = file1.map(lambda (x,y) : (y,x))
file3 = file2.mapValues(lambda x : x.split('/')[-1])

def remove_pucnt(x):
    for i in range (len(x)):
        x[i] = x[i].translate(None, string.punctuation)
        #x[i]= re.sub('[ \t]+' , ' ', x[i])
        
    return (x)

#To remove punctuation 
remove_punct = lambda x : x not in string.punctuation



#creating Rdd with words(keys) without punctuation 
file4 = file3.map(lambda (x, y) : (filter(remove_punct, x), y) )

 
def func(x):
    y = []
    for i in x:
        if i not in y:
            y.append(i)   
    return y
#converting to lower case
file5 = file4.map(lambda (x,y) : (x.lower(), y))

file6 = file5.flatMap(lambda(content,name):map(lambda word : (word,name), content.split()))

file7 = file6.map(lambda (word,name) : ((word,name), 1))

file8 = file7.reduceByKey(lambda count1, count2 : count1+count2)

file9 = file8.map(lambda((word,name), count):(word,(name,count)))

result = file9.groupByKey().mapValues(list)

file10 = result.map(lambda (x,y):(x,func(y)))

for i in file10.collect():
    print i

file10.saveAsTextFile("C:\Users\Krutika\Desktop\myOutputPart2.txt")


   
