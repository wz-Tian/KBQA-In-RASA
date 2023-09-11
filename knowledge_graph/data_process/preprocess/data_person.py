"""
对名人数据进行预处理
@author ZhouZL<zhouzl_2002@163.com>
@date 2023.08.26
@version 1.1m.0
"""

# -*'coding',#utf-8 -*-
import pandas as pd
import re

class DataPerson:
    """
    对名人数据进行预处理

    通过读入已有CSV文件，对数据进行筛选和填充

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
            'person_id',#名人ID
            'person_name',#名人姓名
            'sex',#名人性别
            'person_name_en',#名人英文别名
            'person_name_zh',#名人中文别名
            'birth', #出生日期
            'profession',#职业
            'star',#星座
            'biography',#简介
            'place',#出生地
        )
        cls.list_col_in = (
            'PERSON_ID',#名人ID
            'NAME',#名人名称
            'SEX',#性别
            'NAME_EN',#更多英文名
            'NAME_ZH',#更多中文名
            'BIRTH', #出生日期
            'PROFESSION',#职业
            'CONSTELLATORY',#星座
            'BIOGRAPHY',#简介
            'BIRTHPLACE',#出生地
        )
    def data_read(self):
        """
        从CSV文件中读入数据
        """
        self.data = pd.read_csv(self.path_dataset, encoding='utf-8', dtype=str,nrows=8290)
    
    def data_deal(self):
        """
        对读入的数据进行调整
        """
        
        #对电影数据的空值称行处理
        self.data = self.data.dropna(subset=['PERSON_ID', 'NAME'])
        self.data = self.data.fillna(
            {
            'SEX': '未知',
            'NAME_EN': '无',
            'NAME_ZH': '无',
            'BIRTH': '未知',
            'PROFESSION': '未知',
            'CONSTELLATORY': '未知',
            'BIRTHPLACE': '未知',
            'BIOGRAPHY':'无'
            }
        )
        #对部分数据单独处理
        for row in range(self.data.shape[0]):
            #对电影上映年份信息进行单独截取
            self.data.loc[row,'BIRTH'] = self.data.loc[row,'BIRTH'].split('-')[0]
            #去除空格
            self.data.loc[row,'NAME'] = self.data.loc[row,'NAME'].replace(' ','')
            self.data.loc[row,'SEX'] = self.data.loc[row,'SEX'].replace(' ','')
            self.data.loc[row,'NAME_EN'] = self.data.loc[row,'NAME_EN'].replace(' ','')
            self.data.loc[row,'NAME_ZH'] = self.data.loc[row,'NAME_ZH'].replace(' ','')
            self.data.loc[row,'BIRTH'] = self.data.loc[row,'BIRTH'].replace(' ','')
            self.data.loc[row,'PROFESSION'] = self.data.loc[row,'PROFESSION'].replace(' ','')
            self.data.loc[row,'CONSTELLATORY'] = self.data.loc[row,'CONSTELLATORY'].replace(' ','')
            self.data.loc[row,'BIOGRAPHY'] = self.data.loc[row,'BIOGRAPHY'].replace(' ','')
            self.data.loc[row,'BIRTHPLACE'] = self.data.loc[row,'BIRTHPLACE'].replace(' ','')
            #去掉分隔符
            self.data.loc[row,'NAME'] = self.data.loc[row,'NAME'].replace('·','')
            self.data.loc[row,'NAME_EN'] = self.data.loc[row,'NAME_EN'].replace('·','')
            self.data.loc[row,'NAME_ZH'] = self.data.loc[row,'NAME_ZH'].replace('·','')
            self.data.loc[row,'BIRTH'] = self.data.loc[row,'BIRTH'].replace('·','')
            self.data.loc[row,'PROFESSION'] = self.data.loc[row,'PROFESSION'].replace('·','')
            self.data.loc[row,'CONSTELLATORY'] = self.data.loc[row,'CONSTELLATORY'].replace('·','')
            self.data.loc[row,'BIOGRAPHY'] = self.data.loc[row,'BIOGRAPHY'].replace('·','')
            self.data.loc[row,'BIRTHPLACE'] = self.data.loc[row,'BIRTHPLACE'].replace('·','')
            #处理地区数据
            self.data.loc[row,'BIRTHPLACE'] = self.data.loc[row,'BIRTHPLACE'].replace(' ','').split(',')[0]
            #更换字符
            if not self.data.loc[row,'BIRTHPLACE'].find('台湾') == -1:
                    self.data.loc[row,'BIRTHPLACE'] = '中国台湾'
            else:
                if not self.data.loc[row,'BIRTHPLACE'].find('香港') == -1:
                    self.data.loc[row,'BIRTHPLACE'] = '中国香港'
                else:
                    if not self.data.loc[row,'BIRTHPLACE'].find('澳门') == -1:
                        self.data.loc[row,'BIRTHPLACE'] = '中国澳门'
                    else:
                        if not self.data.loc[row,'BIRTHPLACE'].find('澳门') == -1:
                            self.data.loc[row,'BIRTHPLACE'] = '中国大陆'
            #如果同时存在中文和英文，删除英文
            if self._str_en_zh(self.data.loc[row,'BIRTHPLACE']):
                for ch in self.data.loc[row,'BIRTHPLACE']:
                    if re.match(r'[a-z]+',ch):
                        self.data.loc[row,'BIRTHPLACE'].replace(ch,'')
                        break
        #更改输出数据的列名
        for index,old in enumerate(self.list_col_in):
            self.data = self.data.rename(columns={old:self.list_col_out[index]})
    
    def data_write(self):
        """
        将处理后的数据重新写入文件
        """
        self.data.to_csv(self.path_data,index=None,encoding= 'utf-8',quoting=1)
    
    def data_process(self):
        """
        对读入的数据进行处理
        """
        self.data_read()
        self.data_deal()
        self.data_write()
    
    def _str_en_zh(self,str):
        """
        私有方法：判断字符串是否同时包含英文和中文
        @param str  被查字符串
        @return res 返回结果，如果是则返回true，否则返回false
        """
        res = True
        i = 0
        for ch in str:
            if not u'\u4e00' <= ch <= u'\u9fa5':
                i +=1
                break
        for ch in str:
            if re.match(r'[a-z]+',str):
                i +=1
                break
        if i == 2:
            res = True
        else:
            res = False
        return res

def main():
    file_in = './dataset/person.csv'
    file_out = './data/process/person.csv'
    n = DataPerson(file_in,file_out)
    n.data_process()

if __name__ == '__main__':
    main()