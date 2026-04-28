PS E:\HDD_SHARED_LATO\certus\ciclo 5\bigdata\semana 3\docker-hadoop-master> docker compose up -d
[+] up 34/37
 ✔ Image bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8        Pulled                                   137.8s
 ✔ Image bde2020/hadoop-historyserver:2.0.0-hadoop3.2.1-java8   Pulled                                   137.8s
 ✔ Image bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8        Pulled                                   137.7s
 ✔ Image bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8     Pulled                                   137.6s
 ✔ Image bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8 Pulled                                   137.6s
 ✔ Network docker-hadoop-master_default                         Created                                   0.3s
 ✔ Volume docker-hadoop-master_hadoop_historyserver             Created                                   0.3s
 ✔ Volume docker-hadoop-master_hadoop_namenode                  Created                                                                               
                                                               0.0s
 ✔ Volume docker-hadoop-master_hadoop_datanode                  Created                                                                               
                                                               0.0s
 ✔ Container resourcemanager                                    Created                                                                                             
                                                 2.5s
 ✔ Container historyserver                                      Created                                                                                             
                                                 2.5s
 ✔ Container namenode                                           Created                                                                                             
                                                 2.5s
 ✔ Container namenode                                           Created                                                                                             
                                                      2.5s        r nodemanager                               
 ✔ Container datanode                                           Created                                                              2.5s
 ✔ Container nodemanager                                        Created                                                                                              
                                                2.6s   
PS E:\HDD_SHARED_LATO\certus\ciclo 5\bigdata\semana 3\docker-hadoop-master> docker exec -it namenode bash     

root@87ac465a5adc:/# hdfs dfs -ls /
Found 1 items
drwxr-xr-x   - root supergroup          0 2026-04-08 14:20 /rmstate
root@87ac465a5adc:/# hdfs dfs -ls /
Found 1 items
drwxr-xr-x   - root supergroup          0 2026-04-08 14:20 /rmstate
root@87ac465a5adc:/# ls /tmp
hadoop-root-namenode.pid
hsperfdata_root
jetty-0.0.0.0-9870-hdfs-_-any-6075987885990719074.dir  
texto.txt
root@87ac465a5adc:/# hdfs dfs -mkdir /input
root@87ac465a5adc:/# hdfs dfs -ls /
Found 2 items
drwxr-xr-x   - root supergroup          0 2026-04-08 14:49 /input
drwxr-xr-x   - root supergroup          0 2026-04-08 14:20 /rmstate
root@87ac465a5adc:/# hdfs dfs -put /tmp/text.txt /input
put: `/tmp/text.txt': No such file or directory
root@87ac465a5adc:/# hdfs dfs -put /tmp/texto.txt /inpu
t
2026-04-08 14:52:00,500 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
root@87ac465a5adc:/# hdfs dfs -ls /input
Found 1 items
-rw-r--r--   3 root supergroup       8596 2026-04-08 14:52 /input/texto.txt
root@87ac465a5adc:/# hdfs dfs -cat /input/texto.txt    

root@87ac465a5adc:/# ls /tmp
WordCount.java            jetty-0.0.0.0-9870-hdfs-_-any-6075987885990719074.dir
hadoop-root-namenode.pid  texto.txt
hsperfdata_root
root@87ac465a5adc:/# mv /tmp/WordCount.java /root/
root@87ac465a5adc:/# ls /root
WordCount.java
root@87ac465a5adc:/# cd /root
root@87ac465a5adc:~# ls
WordCount.java
root@87ac465a5adc:~# export HADOOP_CLASSPATH=$(hadoop classpath)
root@87ac465a5adc:~# javac -encoding UTF-8 -classpath $HADOOP_CLASSPATH WordCount.java
root@87ac465a5adc:~# ls
WordCount$IntSumReducer.class    WordCount.class
WordCount$TokenizerMapper.class  WordCount.java
root@87ac465a5adc:~# jar cf wordcount.jar  WordCount*.class
root@87ac465a5adc:~# ls
WordCount$IntSumReducer.class    WordCount.class  wordcount.jar
WordCount$TokenizerMapper.class  WordCount.java
root@87ac465a5adc:~# hadoop jar wordcount.jar WordCount /input /output
2026-04-08 15:21:58,087 INFO client.RMProxy: Connecting to ResourceManager at resourcemanager/172.18.0.4:8032
2026-04-08 15:21:58,522 INFO client.AHSProxy: Connecting to Application History server at historyserver/172.18.0.5:10200
2026-04-08 15:21:59,082 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
2026-04-08 15:21:59,169 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/root/.staging/job_1775658036480_0001
2026-04-08 15:21:59,442 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
2026-04-08 15:22:00,205 INFO input.FileInputFormat: Total input files to process : 1
2026-04-08 15:22:00,277 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
2026-04-08 15:22:00,331 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
2026-04-08 15:22:00,357 INFO mapreduce.JobSubmitter: number of splits:1
2026-04-08 15:22:00,624 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
2026-04-08 15:22:00,684 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1775658036480_0001
2026-04-08 15:22:00,684 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-04-08 15:22:01,091 INFO conf.Configuration: resource-types.xml not found
2026-04-08 15:22:01,092 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-04-08 15:22:02,420 INFO impl.YarnClientImpl: Submitted application application_1775658036480_0001
2026-04-08 15:22:02,521 INFO mapreduce.Job: The url to track the job: http://resourcemanager:8088/proxy/application_1775658036480_0001/
2026-04-08 15:22:02,522 INFO mapreduce.Job: Running job: job_1775658036480_0001
2026-04-08 15:22:21,281 INFO mapreduce.Job: Job job_1775658036480_0001 running in uber mode : false
2026-04-08 15:22:21,284 INFO mapreduce.Job:  map 0% reduce 0%
2026-04-08 15:22:28,755 INFO mapreduce.Job:  map 100% reduce 0%
2026-04-08 15:22:35,854 INFO mapreduce.Job:  map 100% reduce 100%
2026-04-08 15:22:36,881 INFO mapreduce.Job: Job job_1775658036480_0001 completed successfully
2026-04-08 15:22:37,055 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=3114
                FILE: Number of bytes written=463863
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=8697
                HDFS: Number of bytes written=5169
                HDFS: Number of read operations=8
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Launched map tasks=1
                Launched reduce tasks=1
                Rack-local map tasks=1
                Total time spent by all maps in occupied slots (ms)=12456
                Total time spent by all reduces in occupied slots (ms)=34712
                Total time spent by all map tasks (ms)=3114
                Total time spent by all reduce tasks (ms)=4339
                Total vcore-milliseconds taken by all map tasks=3114
                Total vcore-milliseconds taken by all reduce tasks=4339
                Total megabyte-milliseconds taken by all map tasks=12754944
                Total megabyte-milliseconds taken by all reduce tasks=35545088
        Map-Reduce Framework
                Map input records=116
                Map output records=1263
                Map output bytes=13534
                Map output materialized bytes=3106
                Input split bytes=101
                Combine input records=0
                Combine output records=0
                Reduce input groups=514
                Reduce shuffle bytes=3106
                Reduce input records=1263
                Reduce output records=514
                Spilled Records=2526
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=448
                CPU time spent (ms)=3420
                Physical memory (bytes) snapshot=651747328
                Virtual memory (bytes) snapshot=13556219904
                Total committed heap usage (bytes)=586153984
                Peak Map Physical memory (bytes)=440852480
                Peak Map Virtual memory (bytes)=5104611328
                Peak Reduce Physical memory (bytes)=210894848
                Peak Reduce Virtual memory (bytes)=8451608576
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=8596
        File Output Format Counters
                Bytes Written=5169
root@87ac465a5adc:~# hdfs dfs -ls /output        
Found 2 items
-rw-r--r--   3 root supergroup          0 2026-04-08 15:22 /output/_SUCCESS
-rw-r--r--   3 root supergroup       5169 2026-04-08 15:22 /output/part-r-00000
root@87ac465a5adc:~# hdfs dfs -cat /output/part-r-00000
2026-04-08 15:24:47,802 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
A       24
Access  1
After   5
An      5
Apache  1
Artificial      1
Batch   1
Big     2
CAP     1
CPU     2
Cleaning        1
Cloud   2
Continuous      2

root@87ac465a5adc:~# 