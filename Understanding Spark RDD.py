# Databricks notebook source
from pyspark import SparkContext, SparkConf
conf1=SparkConf().setMaster("*").setAppName("First_rdd")

# COMMAND ----------

#sc=SparkContext(conf=conf1)
list1=[3,4,5,6,7,8]

# COMMAND ----------

# Using a parallelize method to create a RDD. 
my_rdd=sc.parallelize(list1)

# COMMAND ----------

#RDD are bit lazy. using collect method to call the output.
my_rdd.collect()

# COMMAND ----------

#take will give the specified no of values/objects from the RDD.
my_rdd.take(3)

# COMMAND ----------

#creating a lambda function to find out the multiplication of the same number. 
#here we used the map method to add the function and then called the output. 
my_rdd.map(lambda num:num*num).collect()

# COMMAND ----------

square_rdd=my_rdd.map(lambda num:num*num).collect()

# COMMAND ----------

#square_rdd.collect()
#To create a key value pair
#square_rdd=my_rdd.map(lambda x:(x,1)).collect()
#square_list = square_rdd.collect()

square_rdd = my_rdd.map(lambda num: num * num)

# Collect the contents of the RDD as a list
square_list = square_rdd.collect()

# Print the list to the console
print(square_list)


# COMMAND ----------

square_rdd=my_rdd.filter(lambda x:x%2==0)
square_list=square_rdd.collect()
print(square_list)

square_rdd=my_rdd.map(lambda x:x%2==0)
square_list=square_rdd.collect()
print(square_list)


# COMMAND ----------

square_rdd=my_rdd.filter(lambda num:num*num)
square_list=square_rdd.collect()
print(square_list)

square_rdd=my_rdd.map(lambda num:num*num)
square_list=square_rdd.collect()
print(square_list)

# COMMAND ----------

sc.range(10).collect()
sc.range(0,end=100,step=5).collect()


# COMMAND ----------

#reduce operation is used to do the aggregation operations. it is an action operation. no need of using collect()
my_rdd.reduce(lambda x,y:x+y)

# COMMAND ----------

my_rdd.fold(0,lambda x,y:x+y)

# COMMAND ----------

blank_rdd=sc.parallelize([1,2])
blank_rdd.reduce(lambda x,y:x+y )

# COMMAND ----------

# difference between map and flat map

# map function do the function 1-1. but flat map  many-many. 
nameList=["Machine Learning", "Deep Learning","Artificial Intellegence"]
rdd_new=sc.parallelize(nameList)
rdd_new.collect()



# COMMAND ----------

nameList=["Machine Learning", "Deep Learning","Artificial Intellegence"]
rdd_new=sc.parallelize(nameList)
rdd_new.flatMap(lambda x:x.split()).collect()

# COMMAND ----------

nameList=["Machine Learning", "Deep Learning","Artificial Intellegence"]
rdd_new=sc.parallelize(nameList)
rdd_new.flatMap(lambda x:x.split()).take(2)
