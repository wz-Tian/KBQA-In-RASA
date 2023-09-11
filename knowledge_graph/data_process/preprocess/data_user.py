"""
对用户数据进行预处理
@author TianWZ
@date 2023.08.26
@version 1.1.0
"""

# -*'coding',#utf-8 -*-
import pandas as pd


class DataUser:
    """
    对用户数据进行预处理

    通过读入已有CSV文件，对数据进行筛选和填充

    Attributes:
        path_dataset(str)       数据集文件所在地址
        path_data(str)          处理后数据输出地址
        list_col_in(list)       数据读取目标CSV文件中需要用到的列名
        list_col_out(list)      数据处理结果CSV文件的列名
    """

    def __init__(cls, path_data, path_node):
        """
        节点信息初始化
        @param  path_dataset    数据集文件所在地址
        @param  path_data       处理后数据输出地址
        """
        cls.path_dataset = path_data
        cls.path_data = path_node
        cls.list_col_out = (
            'user_id', # MD5加密后用户名
            'user_name', # 昵称
        )
        cls.list_col_in = (
            'USER_MD5', # MD5加密后用户名
            'USER_NICKNAME', # 昵称
        )

    def data_read(self):
        """
        从CSV文件中读入数据
        """
        self.data = pd.read_csv(self.path_dataset,encoding='utf-8', dtype=str,nrows=72620)

    def data_deal(self):
        """
        对读入的数据进行调整
        """

        # 对用户数据的空值称行处理
        self.data = self.data.dropna(subset=['USER_MD5'])
        self.data = self.data.fillna(
            {
                'USER_NICKNAME': '无',
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
    file_in = './dataset/users.csv'
    file_out = './data/process/user.csv'
    n = DataUser(file_in, file_out)
    n.data_process()


if __name__ == '__main__':
    main()