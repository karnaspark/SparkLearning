#Persistence:
    Preserving partitions (RDD/DF) in memory or disk
    Why?
        in case of lost partitions, in order to avoid recomputation forom scratch --> performance imporoves
        Lazy Evaluation --> In order to improves 

        r1 --->textfile
        r2.t1 -->r2
        r2.t2 --> r3
        r3.t3 -->r4 
        r4.ac()    --> r4 persist, and also spark keeps tracks of repeated stage and perists automatically

        r4.t4 -->r5
        r5.t5 --> r6
        r6.a2()

        r4.t6 -->r7
        r7.t7 -->r8
        r8.a3()
persistence can be done in memory/disk,  -->by default. memory
                     serialized/deserialized --> in pyspark, objects are always serialized
                     with/withput replication --> we can have max of 2 replications

            Storage options : 
                MEMORY_ONLY  -> only in memory -->default
                DISK_ONLY  -> only in disk
                MEMORY_AND_DISK --> if enough space in memory.. MEMORY_ONLY else DISK_ONLY
                MEMORY_ONLY_2 --> only in memory with 2 copies
                DISK_ONLY_2  --> only in disk with 2 copies
                MEMORY_AND_DISK_2 --> if enough space in memory.. MEMORY_ONLY else DISK_ONLY with 2 copies
                                      what if recomputation time is less than the time of retrieving back up copy from all machines



Athena --> SQL on top of S3 data
            DDL --> Hive
            DML --> ANSI sql, presto sql engine
            All tables are External tables
