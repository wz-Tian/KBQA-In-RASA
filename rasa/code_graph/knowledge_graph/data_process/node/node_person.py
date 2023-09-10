"""
读取并处理名人节点数据
@author ZhouZL<123456789@qq.com>
@date 2023.08.22
@version 1.0.0
"""
# -*'coding',#utf-8 -*-
import pandas as pd

class NodeMovie:
    """
    名人节点数据生成

    通过读入已有CSV文件，对数据进行筛选和填充，生成知识图谱中演员节点的数据

    Attributes:
        path_dataset(str)       数据集文件所在地址
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
            'person_id',#演员ID
            'person_name',#演员姓名
            'sex',#演员性别
            'person_name_en',#演员英文别名
            'person_name_zh',#演员中文别名
            'birth',#出生日期
            'profession',#职业
        )
        cls.list_col_in = (
            'PERSON_ID',#名人ID
            'NAME',#演员名称
            'SEX',#性别
            'NAME_EN',#更多英文名
            'NAME_ZH',#更多中文名
            'BIRTH',#出生日期
            'PROFESSION'#职业
        )
        cls.list_col_delete = (
            'BIOGRAPHY'#简介
            'BIRTHPLACE',#出生地
            'CONSTELLATORY',#星座
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

        #确定读取数据的数量并删除多余的行
        if(self.max_num < 0) or (self.max_num > self.data.shape[0]):
            self.max_num = self.data.shape[0]
        self.data.drop(self.data.index(range(self.max_num,self.data.shape[0])))

        #针对每一行数据中的空值进行处理
        self.data.dropna(subset=['PERSON_ID', 'NAME'])
        self.data.fillna(
            {
            'SEX': '未知', 
            'NAME_EN': '无',
            'NAME_ZH': '无',
            'BIRTH': '未知',
            'PROFESSION': '未知',
            }
        )
        for row in self.data.shape[0]:
                self.data.loc[row,'PROFESSION'] = self.data.loc[row,'PROFESSION'].replace(' ','')
                
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
    file_in = './dataset/movies.csv'
    file_out = './data/movie.csv'
    n = NodeMovie(file_in,file_out,50000)
    n.data_process_movie()

if __name__ == '__main__':
    main()