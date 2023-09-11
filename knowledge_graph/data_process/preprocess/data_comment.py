"""
对名人数据进行预处理
@author ZhouZL<zhouzl_2002@163.com>
@date 2023.08.26
@version 1.1m.0
"""

# -*'coding',#utf-8 -*-
import pandas as pd
import re


class NodeComment:
    """
    对名人数据进行预处理

    通过读入已有CSV文件，对数据进行筛选和填充

    Attributes:
        path_dataset(str)       数据集文件所在地址
        path_data(str)          处理后数据输出地址
        list_col_in(list)       数据读取目标CSV文件中需要用到的列名
        list_col_out(list)      数据处理结果CSV文件的列名
    """

    def __init__(cls, path_dataset, path_data):
        """
        节点信息初始化
        @param  path_dataset    数据集文件所在地址
        @param  path_data       处理后数据输出地址
        @param  max_num         获取数据最大条数
        """
        cls.path_dataset = path_dataset
        cls.path_data = path_data
        cls.list_col_out = (
            'comment_id',  # 评论ID
            'user_id',  # 用户名MD5
            'movie_id',  # 电影ID
            'content',  # 评论
            'votes',  # 评分
            'comment_time',  # 评论日期
            'rating',  # 豆瓣投票数
        )
        cls.list_col_in = (
            'COMMENT_ID',
            'USER_MD5',
            'MOVIE_ID',
            'CONTENT',
            'VOTES',
            'COMMENT_TIME',
            'RATING',
        )

    def data_read(self):
        """
        从CSV文件中读入数据
        """
        # self.data = pd.read_csv(self.path_dataset,dtype={"COMMENT_ID":str,
        #                                                   "USER_MD5":str,
        #                                                   "MOVIE_ID":str,
        #
        #                                                   "CONTENT":str,"VOTES":str,
        #                                                   "COMMENT_TIME":str,"RATING":str})

        # self.data = pd.read_csv(self.path_dataset,encoding='utf-8',dtype=str,nrows=500000)
        self.data = pd.read_csv(self.path_dataset, encoding='utf-8', dtype=str)
        self.data = self.data.drop_duplicates(subset=['MOVIE_ID'])
        self.data = self.data.sort_values(by="MOVIE_ID", ascending=True)


    def data_deal(self):
        """
        对读入的数据进行调整
        """

        # 对电影数据的空值称行处理
        # self.data = self.data.dropna()
        self.data = self.data.dropna(subset=['COMMENT_ID','USER_MD5','MOVIE_ID'])
        self.data = self.data.fillna(
            {
                'CONTENT': '无',
                'VOTES':'无',
                'COMMENT_TIME': '无',
                'RATING':'无',
            }
        )

        # 更改输出数据的列名
        for index, old in enumerate(self.list_col_in):
            self.data = self.data.rename(columns={old: self.list_col_out[index]})


    def data_write(self):
        """
        将处理后的数据重新写入文件
        """
        self.data.to_csv(self.path_data, index=None,encoding= 'utf-8',quoting=1)

    def data_process(self):
        """
        对读入的数据进行处理
        """
        self.data_read()
        self.data_deal()
        self.data_write()


def main():
    file_in = './dataset/comments.csv'
    file_out = './data/process/comment.csv'
    n = NodeComment(file_in, file_out)
    n.data_process()


if __name__ == '__main__':
    main()