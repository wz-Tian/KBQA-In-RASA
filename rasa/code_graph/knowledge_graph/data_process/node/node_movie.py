"""
读取并处理电影节点数据
@author ZhouZL<123456789@qq.com>
@date 2023.08.22
@version 1.0.0
"""
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import copy

class NodeMovie:
    """
    电影节点数据生成

    通过读入已有CSV文件，对数据进行筛选和填充，生成知识图谱中电影节点的数据

    Attributes:
        path_set(str)           数据集文件所在地址
        path_data(str)          处理后数据输出地址
        list_col_in(list)       数据读取目标CSV文件中需要用到的列名
        list_col_out(list)      数据处理结果CSV文件的列名
        list_col_delete(list)   数据读取目标CSV文件的需要删除的列
        max_num(int)            获取数据最大条数
    """

    def __init__(cls,path_dataset,path_data,max_num = -1):
        """
        节点信息初始化
        @param  path_dataset    数据集文件所在地址
        @param  path_data       处理后数据输出地址
        @param  max_num         获取数据最大条数
        """
        cls.path_dataset = path_dataset
        cls.path_data = path_data
        cls.max_num = max_num
        cls.list_col_out = (
            'movie_id',#电影ID
            'movie_name',#名称
            'alias',#别名
            'mins',#电影时长
            'storyline',#电影情节描述
            'score',#豆瓣评分
            'votes',#豆瓣投票数
        )
        cls.list_col_in = (
            'MOVIE_ID',#电影ID
            'NAME',#名称
            'ALIAS',#别名
            'MINS',#电影时长
            'STORYLINE',#电影情节描述
            'DOUBAN_SCORE',#豆瓣评分
            'DOUBAN_VOTES',#豆瓣投票数
        )
        cls.list_col_delete = (
            'COVER',#封面图片地址
            'ACTORS',#主演
            'DIRECTORS',#导演
            'GENRES',#类型
            'REGIONS',#地区
            'LANGUAGES',#电影使用语言
            'OFFICIAL_SITE',#地址
            'RELEASE_DATE',#上映日期
            'YEAR'#上映年份
            'TAGS',#标签
            'IMDB_ID',#IMDbID
            'SLUG',#加密的url，可忽略
            'ACTOR_IDS',#演员与PERSON_ID的对应关系,多个演员采用“\|”符号分割，格式“演员A:ID\|演员B:ID”
            'DIRECTOR_IDS',#导演与PERSON_ID的对应关系,多个导演采用“\|”符号分割，格式“导演A:ID\|导演B:ID”；
        )
    
    def data_read(self):
        """
        从CSV文件中读入数据
        """
        self.data = pd.read_csv(self.path_dataset,engine='python')
    
    def data_deal(self):
        """
        对读入的数据进行调整
        """
        #删除读入数据中用不到的列
        for col in self.list_col_delete:
            self.data.drop(col,axis=1)

        #确定读取数据的数量,并删除多余的行
        if(self.max_num < 0) or (self.max_num > self.data.shape[0]):
            self.max_num = self.data.shape[0]
        self.data.drop(self.data.index(range(self.max_num,self.data.shape[0])))

        #针对每一行数据中的空值进行处理
        self.data.dropna(subset=['MOVIE_ID', 'NAME'])
        self.data.fillna(
            {
            'ALIAS': '无',
            'STORYLINE': '无',
            'MINS': '未知',
            }
        )
        #更改输出数据的列名
        for index,old in enumerate(self.list_col_in):
            self.data.rename(columns={old:self.list_col_out[index]})

    def data_write(self):
        """
        将处理后的数据重新写入文件
        """
        self.data.to_csv(self.path_data)
    
    def data_process_movie(self):
        """
        对读入的数据进行处理
        """
        self.data_read()
        self.data_deal()
        self.data_write()

def main():
    file_in = './dataset/person.csv'
    file_out = './data/person.csv'
    n = NodeMovie(file_in,file_out)
    n.data_process_movie()

if __name__ == '__main__':
    main()