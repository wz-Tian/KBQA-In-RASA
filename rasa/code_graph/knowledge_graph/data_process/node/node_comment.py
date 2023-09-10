"""
读取并处理评论节点数据
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
    评论节点数据生成

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
            'comment_id',#评论ID
            'user_id',#用户ID
            'movie_id',#电影ID
            'content',#评论内容
            'votes',#评论赞同数
            'comment_time'#评论时间
        )
        cls.list_col_in = (
            'COMMENT_ID',#评论ID
            'USER_ID',#用户ID
            'MOVIE_ID',#电影ID，对应豆瓣的DOUBAN_ID
            'CONTENT',#评论内容
            'VOTES',#评论赞同数
            'COMMENT_TIME'#评论时间
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
         #确定读取数据的数量,并删除多余的行
        if(self.max_num < 0) or (self.max_num > self.data.shape[0]):
            self.max_num = self.data.shape[0]
        self.data.drop(self.data.index(range(self.max_num,self.data.shape[0])))

        #针对每一行数据中的空值进行处理
        self.data.dropna(subset=['MOVIE_ID', 'COMMENT_ID'])
        self.data.fillna(
            {
            'CONTENT': '无',
            'COMMENT_TIME': '无',
            'USER_ID': '未知',
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
    file_in = './dataset/comment.csv'
    file_out = './data/comment.csv'
    n = NodeMovie(file_in,file_out)
    n.data_process_movie()

if __name__ == '__main__':
    main()