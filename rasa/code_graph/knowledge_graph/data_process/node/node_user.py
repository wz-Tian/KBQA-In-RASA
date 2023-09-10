"""
读取并处理用户节点数据
@author ZhouZL<123456789@qq.com>
@date 2023.08.22
@version 1.0.0
"""
# -*'coding',#utf-8 -*-
import pandas as pd
import numpy as np
import copy

class NodeMovie:
    """
    用户节点数据生成

    通过读入已有CSV文件，对数据进行筛选和填充，生成知识图谱中演员节点的数据

    Attributes:
        path_dataset(str)       数据集文件所在地址
        path_data(str)          处理后数据输出地址
        list_col_in(list)       数据读取目标CSV文件中需要用到的列名
        list_col_out(list)      数据处理结果CSV文件的列名
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
            'user_id',#用户ID
            'user_name',#用户姓名昵称
        )
        cls.list_col_in = (
            'USER_ID',#豆瓣用户ID
            'USER_NICKNAME',#评论用户昵称
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
        self.data.dropna(subset=['USER_ID', 'USER_NICKNAME'])

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
    file_in = './dataset/users.csv'
    file_out = './data/user.csv'
    n = NodeMovie(file_in,file_out,50000)
    n.data_process_movie()

if __name__ == '__main__':
    main()