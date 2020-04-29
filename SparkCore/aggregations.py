"""
Aggregatins are reduce, fold, aggregate  (Actions)
pair RDD -> groupByKey, reduceByKey, foldByKey,aggregateByKey ,combineByKey (transformations)
"""
# ----------------------------------------------------------------------------------------
reduce(lambda v1,v2: v1+v2)
rdd.reduce(fucntion)
computation will ahpppen at partition level followed by on the results of the partitions

[1, 2, 3, 4]  --> 1+2->3+3>6+4-> 10
[5, 6, 7, 8, 9] --> 5+6->11+7 ->18+8-> 26 +9 -> 35
[10, 11, 12, 13, 14] ->10+11->21+12 -> 33+13->46+14 -> 60

[10,35,60] -> 10+35 -> 45+60 ->105
# ---------------------------------------------------------------------------------------
r1.fold(3,lambda v1,v2:v1+v2)
rdd.fold(zv,function)
zv can be used as a reference (initial value)

[1, 2, 3, 4]  -> 3+1=4+2=6+3=9+4=13
[5, 6, 7, 8, 9] --> 3+5=8+6=14+7=21+8=29+9=38
[10, 11, 12, 13, 14] -> 3+10=13+11=24+12=36+13=49+14=63

[13,38,63] -> 3+13=16+38=54+63 =117
# ----------------------------------------------------------------------------------------
r1.aggregate(3,lambda v1,v2:v1+v2,lambda v1,v2:v1-v2)
rdd.aggregate(zv, f1,f2)

function1 will be applied at partition level
function 2 will be applied on the results of the partitions

[1, 2, 3, 4]  -> 3+1=4+2=6+3=9+4=13
[5, 6, 7, 8, 9] --> 3+5=8+6=14+7=21+8=29+9=38
[10, 11, 12, 13, 14] -> 3+10=13+11=24+12=36+13=49+14=63

[13,38,63] --> 3-13 = -10-38=-49-63=-111
# ------------------------------------------------------------------------------------------------
What type of logics/fucntions are not recommended in the above aggregations
A+B = B+A --> computation of A followed by computation of B   generates same result as computation of B followed by computation of A (Commutative)
(A+B)+c = A+(B+C)  --> Associative 

Addition   --> true
Subtraction ->not 
multiplciation--> true
division -->not
# -------------------------------------------------------------------------------------------------
pair RDD 
    Records in RDD as pairs tuple of 2 elements)
    by default, first element of a pair will be treated as key and second element as value

l2 = [("a",20),("b",30),("a",10),("c",40),("b",15),("a",25),("b",35),("a",50),("c",45),("b",55)]

prdd.glom().collect()


prdd.partitionBy(3, part)

def part(key):
    if key == "a":
        return 1
    elif key == "b":
        return 2
    else:
        return 3    

# -------------------------------------------------------
prdd.groupByKey().map(lambda rec: (rec[0],list(rec[1]))).collect()

#------------------------------------------------------------------------------------

l3 = ["spark is a framework","hadoop is a framework","hive is a warehouse","spark is dis. fraemwork","hadoop is a dis, storage and proce. framwork"]
prdd2.map(lambda x: (x.split(" ")[0],x)).groupByKey().map(lambda x: (x[0],list(x[1]))).collect()

l4 = [("a",1,2,3),("b",3,4,2),("a",4,5,6),("c",3,4,5),("b",7,6,5)]
prd1.groupBy(lambda x: x[0]).collect()
#------------------------------------------------------------------------------------------------------
prdd.reduceByKey(func)


('a', 20), ('b', 30), ('a', 10), ('c', 40), ('b', 15) --> (a,(20,10)), (b,(30,15)), (c,(40)) --> (a, 30), (b,45) (c,40)
('a', 25), ('b', 35), ('a', 50), ('c', 45), ('b', 55) --> (a,(25,50)), (b,(35,55)), (c,(45)) --> (a,75) (b,90) (c,45)

(a, 30), (b,45) (c,40)  (a,75) (b,90) (c,45) --> (a,30,75) (b,45,90) (c,40,45) --> (a,105), (b,135) (c,85)
# ---------------------------------------------------------------------------------------------------
foldByKey(zv, function)
prdd.foldByKey(5,lambda x,y: x+y).collect()

('a', 20), ('b', 30), ('a', 10), ('c', 40), ('b', 15) --> (a,(5,20,10)), (b,(5,30,15)), (c,(5,40)) --> (a, 35), (b,50) (c,45)
('a', 25), ('b', 35), ('a', 50), ('c', 45), ('b', 55) --> (a,(5,25,50)), (b,(5,35,55)), (c,(5,45)) --> (a,80) (b,95) (c,50)

(a, 35), (b,50) (c,45) (a,80) (b,95) (c,50) --> (a,35,80)  (b,50,95) (c, 45,50) --> (a,115) (b,145) (c,95)
#--------------------------------------------------------------------------------------------------
aggregateByKey(zv, function1, fucntion2)
prdd.aggregateByKey(5,lambda x,y: x+y,lambda x,y: x-y).collect()

('a', 20), ('b', 30), ('a', 10), ('c', 40), ('b', 15) --> (a,(5,20,10)), (b,(5,30,15)), (c,(5,40)) --> (a, 35), (b,50) (c,45)
('a', 25), ('b', 35), ('a', 50), ('c', 45), ('b', 55) --> (a,(5,25,50)), (b,(5,35,55)), (c,(5,45)) --> (a,80) (b,95) (c,50)

(a, 35), (b,50) (c,45) (a,80) (b,95) (c,50) --> (a,35,80)  (b,50,95) (c, 45,50) --> (a,-45) (b,-45) (c,-5)
#---------------------------------------------------------------------------------------------------














