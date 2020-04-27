#Word count program

#Find the number of occurances of each word in a given file

rd1 = sc.textFile("inputfile")
# Solution 1:
rd1.flatMap(lambda rec: rec.split(" ")).map(lambda x:(x,1)).groupByKey().map(lambda x:(x[0],sum(x[1]))).sortByKey().collect()

# Solution 2:
rd1.flatMap(lambda rec: rec.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda v1,v2: v1+v2).sortByKey().collect()

"""
#We should avoid using groupByKey(), recommneded to use reduceByKey()
rdd = [ab cd ab abc cd  ab abc xy abc] 

p1 = [ab cd ab abc cd]
p2 = [ab abc xy abc]

groupByKey().map(lambda x:(x[0],sum(x[1])))

p1 = [ab cd ab abc cd]
p2 = [ab abc xy abc]

[ab cd ab abc cd  ab abc xy abc] --> (ab,(1,1,1),(cd,(1,1)),(abc,(1,1,1)),(xy,(1))) --> ((ab,3),(cd,2),(abc,3),(xy,1))

It does not perform any operation at partition level. entire data from all partitions must be brought(shuffling) into a single memory location.
#------------------------------------------------------------------------------------------------------------------------
reduceByKey(lambda v1,v2:v1+v2)

p1 = [ab cd ab abc cd] ->(ab,(1,1)) , (cd,(1,1)), (abc,1) --> (ab,2), (cd,2), (abc,1)
p2 = [ab abc xy abc] --> (ab,1), (abc, (1,1)), (xy,1) --> (ab,1), (abc,2), (xy,1)

(ab,2), (cd,2), (abc,1) , (ab,1), (abc,2), (xy,1) --> (ab,(2,1)) , (cd, 2) , (abc,(1,2)), (xy,1) --> (ab,3) , (cd,2) ,(abc,3), (xy,1)

(xy,(1,2,3,2,1,3,2,42,4,24)) --> 
1+2 = 3
3+3 = 6
6+2 = 8

groupByKey ,reduceByKey, sortByKey -->   Wide transformations

Spark job execution can be viewd as a Graph called as DAG (Direct Acyclic Graph)
job -> action
"""