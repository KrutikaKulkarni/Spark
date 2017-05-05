# -*- coding: utf-8 -*-
"""
Created on Sat Apr 01 13:56:24 2017

@author: KrutikaKulkarni
"""
#  import string

from pyspark import SparkConf, SparkContext 

conf = SparkConf().setMaster("local[*]").setAppName("InvertedIndex")
sc = SparkContext(conf = conf)
 
file1 = sc.wholeTextFiles("C:\Users\Krutika\Desktop\Shakespeare\*")
file2 = file1.map(lambda (x,y) : (y,x))
file3 = file2.mapValues(lambda x : x.split('/')[-1])
#filen = file3.map(lambda (x, y) : (x.split("\n")))
"""
def remove_pucnt(x):
    for i in range (len(x)):
        x[i] = x[i].translate(None, string.punctuation)
        #x[i]= re.sub('[ \t]+' , ' ', x[i])
        
    return (x)
"""
#To remove punctuation 
#remove_punct = lambda x : x not in string.punctuation



#creating Rdd with words(keys) without punctuation 
#file4 = file3.map(lambda (x, y) : (filter(remove_punct, x), y) )

#converting to lower case 
def func(x):
    y = []
    for i in x:
        if i not in y:
            y.append(i)   
    return y

file4 = file3.map(lambda (x,y) : (x.lower(), y)).filter(lambda (x,y) : (x) > 0)

file5 = file4.flatMap(lambda (content,filename) : map(lambda word : (word,filename), content.split()))

result = file5.groupByKey().mapValues(list)
file6 = result.map(lambda (x,y):(x,func(y)))
#result = file5.reduceByKey(lambda word, filename : str(word))
#print result.collect()
for i in file6.collect():
    print i
file6.saveAsTextFile("C:\Users\Krutika\Desktop\Shakespeare\myOutput.txt")

#print result.distinct().collect()
   
