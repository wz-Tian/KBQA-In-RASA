"""
根据豆瓣TOP250电影信息搭建知识图谱
@author ZhouZL<123456789@qq.com>
@date 2023.08.22
@version 1.0.0
"""
import pandas as pd
from py2neo import Graph,Node,Relationship

## 连接图形库，配置neo4j
graph = Graph("http://localhost:7474/browser/",auth = ('neo4j','20021013'),name='neo4j')
# 清空全部数据
graph.delete_all()
# 开启一个新的事务
graph.begin()

## csv源数据读取
storageData = pd.read_csv('./knowledge_graph/movieInfo.csv',encoding = 'utf-8')
# 获取所有列标签
columnLst = storageData.columns.tolist()
# 获取数据数量
num = storageData.shape[0]

# KnowledgeGraph知识图谱构建(以电影为主体构建的知识图谱)
for i in range(num):

    # 为每部电影构建属性字典
    dict = {}
    for column in columnLst:
        if storageData.loc[i,column] == '黑客帝国3：矩阵革命' or storageData.loc[i,column] == '黑客帝国2：重装上阵':
            storageData.loc[i,'actor'] = '演职人员不详'
        dict[column] = storageData.loc[i,column]
    print(dict)
    node1 = Node('movie',name = storageData.loc[i,'title'],**dict)
    graph.merge(node1,'movie','name')

    # 去除所有的title结点
    dict.pop('title')
    ## 分界点以及关系
    for key,value in dict.items():
        ## 建立分结点
        node2 = Node(key,name = value)
        graph.merge(node2,key,'name')
        ## 创建关系
        rel = Relationship(node1,key,node2)
        graph.merge(rel)