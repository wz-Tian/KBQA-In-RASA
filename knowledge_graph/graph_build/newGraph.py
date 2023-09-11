"""
根据数十万部电影信息搭建知识图谱
@author wz_Tian
@date 2023.08.28
@version 1.0.0
"""
import pandas as pd
from py2neo import Graph, Node, Relationship, NodeMatcher

## 连接图形库，配置neo4j
graph = Graph("http://localhost:7474/browser/",auth = ('neo4j','123456'),name='neo4j')
# 清空全部数据
graph.delete_all()
# 开启一个新的事务
graph.begin()

## csv源数据读取
storageData_comment = pd.read_csv('data/process/comment_final.csv',encoding = 'utf-8',dtype=str)
storageData_movie = pd.read_csv('data/process/movie_final.csv',encoding = 'utf-8',dtype=str)
storageData_person = pd.read_csv('data/process/person.csv',encoding = 'utf-8',dtype=str)
# storageData_user = pd.read_csv('data/process/user.csv',encoding = 'utf-8',dtype=str)
# 获取所有列标签
columnLst_comment = storageData_comment.columns.tolist()
columnLst_movie = storageData_movie.columns.tolist()
columnLst_person = storageData_person.columns.tolist()
# columnLst_user = storageData_user.columns.tolist()
# 获取数据数量
num_comment = storageData_comment.shape[0]
num_movie = storageData_movie.shape[0]
num_person = storageData_person.shape[0]
# num_user = storageData_user.shape[0]

# file_out1 = './data/process/CommentAndUser.csv'
# file_out_movie = './data/process/MovieCommentAndUser.csv'
# file_out_movieFinal = './data/process/movie_Final.csv'
# file_out_commentFinal = './data/process/comment_final.csv'
#
# df_id = storageData_comment.user_id
# storageData_comment = storageData_comment.drop('user_id',axis=1)
# storageData_comment.insert(6,'user_id',df_id)
#
# new_comment = pd.merge(storageData_comment,storageData_user,on='user_id',how='inner')
# new_comment.to_csv(file_out1, index=None,encoding= 'utf-8',quoting=1)
#
# new_movie = pd.merge(storageData_movie,new_comment,on='movie_id',how='inner')
# new_movie.to_csv(file_out_movie, index=None,encoding= 'utf-8',quoting=1)
#
# comment_final = new_movie.iloc[:,14:]
# movie_final = new_movie.iloc[:,:14]
#
# comment_final.to_csv(file_out_commentFinal, index=None,encoding= 'utf-8',quoting=1)
# movie_final.to_csv(file_out_movieFinal, index=None,encoding= 'utf-8',quoting=1)

print(storageData_comment.shape)
print(storageData_movie.shape)
print(storageData_person.shape)
# print(storageData_user.shape)


dict_comment = {}
dict_movie = {}
dict_person = {}


for i in range(num_person):

    for column in columnLst_person:
        dict_person[column] = storageData_person.loc[i, column]

    node3 = Node('person', name=storageData_person.loc[i, 'person_name'], **dict_person)
    graph.merge(node3,'person','name')
    for key, value in dict_person.items():
        if key == 'star':
            node_1 = Node(key, name=value)
            graph.merge(node_1, key, 'name')
            rel = Relationship(node3, '星座', node_1)
            graph.merge(rel)
        if key == 'profession':
            node_2 = Node(key, name=value)
            graph.merge(node_2, key, 'name')
            rel = Relationship(node3, '职业', node_2)
            graph.merge(rel)

for i in range(num_movie):

    for column in columnLst_movie:
        dict_movie[column] = storageData_movie.loc[i, column]

    node2 = Node('movie', name=storageData_movie.loc[i, 'movie_name'], **dict_movie)
    graph.merge(node2, 'movie', 'name')

    for key, value in dict_movie.items():
        if key == 'languages':
            node_1 = Node(key,name=value)
            graph.merge(node_1, key, 'name')
            rel = Relationship(node2, '使用', node_1)
            graph.merge(rel)
        if key == 'tag':
            node_2 = Node(key, name=value)
            graph.merge(node_2, key, 'name')
            rel = Relationship(node2, '拥有', node_2)
            graph.merge(rel)
        if key == 'category':
            node_3 = Node(key, name=value)
            graph.merge(node_3, key, 'name')
            rel = Relationship(node2, '属于', node_3)
            graph.merge(rel)
        if key == 'regions':
            node_4 = Node(key, name=value)
            graph.merge(node_4, key, 'name')
            rel = Relationship(node2, '拍摄于', node_4)
            graph.merge(rel)
        if key == 'year':
            node_5 = Node(key, name=value)
            graph.merge(node_5, key, 'name')
            rel = Relationship(node2, '处于', node_5)
            graph.merge(rel)
        if key == 'storyline':
            node_6 = Node(key, name=value)
            graph.merge(node_6, key, 'name')
            rel = Relationship(node2, '简介', node_6)
            graph.merge(rel)

    node1 = Node('comment', name=storageData_comment.loc[i, 'comment_id'], **dict_comment)
    graph.merge(node1,'comment','name')
    r = Relationship(node2, "评论", node1)
    graph.merge(r)

    matcher = NodeMatcher(graph)
    actors = dict_movie['actor_list'].split("|")

    actor_name = actors[0].split("：")[0]
    node_actor = matcher.match("person", name=str(actor_name)).first()
    print(node_actor)
    if node_actor == None:
        continue
    else:
        rel = Relationship(node2, '出演人员', node_actor)
        graph.merge(rel)

# for i in range(num_comment):
#
#     for column in columnLst_comment:
#         dict_comment[column] = storageData_comment.loc[i, column]
#
#     node1 = Node('comment', name=storageData_comment.loc[i, 'comment_id'], **dict_comment)
#     graph.merge(node1,'comment','name')
#     node2 = Node('movie', name=storageData_movie.loc[i, 'movie_name'], **dict_movie)
#     r = Relationship(node2, "被评论", node1)
#     graph.merge(r)

    # graph.merge(node1, 'movie', 'name')

    # # 去除所有的title结点
    # dict.pop('movie_name')
    # ## 分界点以及关系
    # for key,value in dict.items():
    #     ## 建立分结点
    #     node2 = Node(key,name = value)
    #     graph.merge(node2,key,'name')
    #     ## 创建关系
    #     rel = Relationship(node1,key,node2)
    #     graph.merge(rel)








# for i in range(num_user):
#
#     dict = {}
#     for column in columnLst_user:
#         dict[column] = storageData_user.loc[i, column]
#
#     node4 = Node('user', name=storageData_user.loc[i, 'user_name'], **dict)
#     graph.create(node4)





# for i in range(num):
#
#     dict = {}
#     for column in columnLst:
#         dict[column] = storageData_comment.loc[i, column]
#
#     node1 = Node('comment', name=storageData_comment.loc[i, 'comment_id'], **dict)
#     graph.create(node1)
#
# for i in range(num):
#
#     dict = {}
#     for column in columnLst:
#         dict[column] = storageData_comment.loc[i, column]
#
#     node1 = Node('comment', name=storageData_comment.loc[i, 'comment_id'], **dict)
#     graph.create(node1)