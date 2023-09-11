# KBQA-In-RASA

知识图谱部分：	

​	1、首先下载neo4j图数据库（版本4.4.10），配置好环境和环境变量，然后启动neo4j图数据库

​	2、运行knowledge_graph/graph_build文件夹下的KnowledgeGraph.py文件，15000部电影信息已通过数据清洗处理好，存放在newMovieInfo.csv中

​	3、运行完后即可再Neo4j客户端看到知识图谱，同时导出dump文件用于问答机器人



问答机器人部分：