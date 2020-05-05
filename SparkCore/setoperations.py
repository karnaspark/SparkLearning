"""
Set operations
1. Intersection
2. Minus (subtract)
3. Union
4. Cross
"""
rd1 = sc.textFile("s3://datasets-spark-learning/flat_files/calllogsdata")
import re

def find_status(rec):
    status_pattern = re.compile(r"[A-Z]+")
    status = status_pattern.search(rec)
    return status.group(0)

rd1.map(lambda rec: find_status(rec)).distinct().take(5)   
dropped_rec= rd1.filter(lambda rec: "DROPPED" in find_status(rec))
failed_rec= rd1.filter(lambda rec: "FAILED" in find_status(rec))

failed_rec.count() + dropped_rec.count() + success_rec.count()
success_rec.union(dropped_rec).count()

#union:
    success_rec.union(dropped_rec).union(failed_rec).getNumPartitions()
    # does not involve shuffling. it just merges the data upon an action
    # duplicates are not addresses
    
#intersection:
    rd1.intersection(success_rec).count()
    # involves shuffling
    rd2.intersection(success_rec).getNumPartitions()

# subtract:
    rd1.subtract(dropped_rec) # records in rd1 but not in dropped_rec 
    # involves shuffling

def part(x):
    yield len(list(x))

# cross:
    # cartesian  
    r1.cartesian(r2).collect()
    # no shuffling
    # number of partitins = multiplication of the input rdd partitions