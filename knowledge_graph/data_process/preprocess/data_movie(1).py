"""
对movie所有相关数据进行数据预处理
@author ZhouZL<zhouzl_2002@163.com>
@date 2023.08.30
@version 2.0.0
"""
# -*- coding: utf-8 -*-
import re
import pandas as pd

class DataMovie:
    """
    电影数据预处理

    通过读入已有CSV文件，对数据进行筛选和填充，完成对电影数据的清洗

    Attributes:
        path_set(str)           数据集文件所在地址
        path_data(str)          处理后数据输出地址
        list_col_in(list)       数据读取目标CSV文件中需要用到的列名
        list_col_out(list)      数据处理结果CSV文件的列名
        list_col_delete(list)   数据读取目标CSV文件的需要删除的列
        max_num(int)            获取数据最大条数
    """

    def __init__(cls,path_dataset,path_data):
        """
        节点信息初始化
        @param  path_dataset    数据集文件所在地址
        @param  path_data       处理后数据输出地址
        """
        cls.path_dataset = path_dataset
        cls.path_data = path_data
        cls.list_col_out = (
            'title',#名称
            'rate',#豆瓣评分
            'num',#豆瓣投票数
            'info',#电影情节描述
            'director',#导演
            'actor',#主演
            'time',#上映日期
            'country',#地区
            'type',#类型
        )
        cls.list_col_in = (
            'NAME',#名称
            'DOUBAN_SCORE',#豆瓣评分
            'DOUBAN_VOTES',#豆瓣投票数
            'STORYLINE',#电影情节描述
            'DIRECTORS',#导演
            'ACTORS',#主演
            'RELEASE_DATE',#上映日期
            'REGIONS',#地区
            'GENRES',#类型
        )
        cls.list_col_delete = (
            'MOVIE_ID',#电影ID
            'DIRECTOR_IDS',#导演与PERSON_ID的对应关系,多个导演采用“\|”符号分割，格式“导演A:ID\|导演B:ID”；
            'ACTOR_IDS',#演员与PERSON_ID的对应关系,多个演员采用“\|”符号分割，格式“演员A:ID\|演员B:ID”
            'TAGS',#标签
            'MINS',#电影时长
            'ALIAS',#别名
            'LANGUAGES',#电影使用语言
            'COVER',#封面图片地址
            'YEAR',#上映年份
            'IMDB_ID',#IMDbID
            'OFFICIAL_SITE',#官网地址
            'SLUG',#加密的url，可忽略
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
        #删除用不到的列
        for col in self.list_col_delete:
            self.data = self.data.drop(col,axis=1)

        #对电影名称的空值称行处理
        self.data.dropna(subset=['NAME'])

        #只保留前15000行
        self.data = self.data.head(55000)

        #对空值进行填充
        self.data = self.data.fillna(
            {
            'DOUBAN_SCORE':'0.0',#豆瓣评分
            'DOUBAN_VOTES':'0',#豆瓣投票数
            'STORYLINE': '暂无',
            'DIRECTORS':'未知',
            'ACTORS':'未知',
            'RELEASE_DATE': '未知',
            'REGIONS': '未知',
            'GENRES': '无',
            }
        )

        #对部分数据单独处理
        for row in range(self.data.shape[0]):
            #删除所有空格
            self.data.loc[row,'NAME'] = self.data.loc[row,'NAME'].replace(' ','')
            self.data.loc[row,'STORYLINE'] = self.data.loc[row,'STORYLINE'].replace(' ','')
            self.data.loc[row,'DIRECTORS'] = self.data.loc[row,'DIRECTORS'].replace(' ','')
            self.data.loc[row,'ACTORS'] = self.data.loc[row,'ACTORS'].replace(' ','')
            self.data.loc[row,'RELEASE_DATE'] = self.data.loc[row,'RELEASE_DATE'].replace(' ','')
            self.data.loc[row,'REGIONS'] = self.data.loc[row,'REGIONS'].replace(' ','')
            self.data.loc[row,'GENRES'] = self.data.loc[row,'GENRES'].replace(' ','')

            #对投票人数进行处理
            if str(self.data.loc[row,'DOUBAN_VOTES']) == '0.0' or str(self.data.loc[row,'DOUBAN_VOTES']) == '':
                self.data.loc[row,'DOUBAN_VOTES'] = '0人'
            else:
                self.data.loc[row,'DOUBAN_VOTES'] = str(self.data.loc[row,'DOUBAN_VOTES']).replace('.0','') + '人'
            
            #获取导演列表中的第一个人
            self.data.loc[row,'DIRECTORS'] = self.data.loc[row,'DIRECTORS'].split('/')[0]
            
            #获取演员列表中第一个人
            self.data.loc[row,'DIRECTORS'] = self.data.loc[row,'DIRECTORS'].split('/')[0]

            #对电影上映年份信息进行单独截取
            self.data.loc[row,'RELEASE_DATE'] = self.data.loc[row,'RELEASE_DATE'].split('-')[0]
            
            #获取地区列表中的第一个
            self.data.loc[row,'REGIONS'] = self.data.loc[row,'REGIONS'].split('/')[0]
            #删除同时含有英文和中文的地区的英文部分
            if self._str_en_zh(self.data.loc[row,'REGIONS']):
                for ch in self.data.loc[row,'REGIONS']:
                    if re.match(r'[a-z]',ch) or re.match(r'[A-Z]',ch):
                        self.data.loc[row,'REGIONS'] = self.data.loc[row,'REGIONS'].replace(ch,'')
        
            #获取类别列表中的第一个
            self.data.loc[row,'GENRES'] = self.data.loc[row,'GENRES'].split('/')[0]
            
        #更改输出数据的列名
        for index,old in enumerate(self.list_col_in):
            self.data = self.data.rename(columns={old:self.list_col_out[index]})
        
        #数据衔接
        df = pd.read_csv('./dataset/movieinfo.csv')
        order = df.columns
        #对导演和演员的人名进行处理
        for row2 in range(df.shape[0]):
            if self.data.loc[row2,'title'] == '黑客帝国2：重装上阵' or self.data.loc[row2,'title'] == '黑客帝国3：矩阵革命':
                self.loc[row2,'actor'] = '未知'
            if self._str_en_zh(str(df.loc[row2,'actor'])):
                for ch in df.loc[row2,'actor']:
                    if re.match(r'[a-z]',ch) or re.match(r'[A-Z]',ch):
                        df.loc[row2,'actor'] = df.loc[row2,'actor'].replace(ch,'')
            if self._str_en_zh(str(df.loc[row2,'director'])):
                for ch in df.loc[row2,'director']:
                    if re.match(r'[a-z]',ch) or re.match(r'[A-Z]',ch):
                        df.loc[row2,'director'] = df.loc[row2,'director'].replace(ch,'')
        self.data = self.data[order]
        #数据衔接
        self.data = pd.concat([df, self.data], sort=True, join='outer')
        self.data = self.data.drop_duplicates(subset=['title'])

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
    
    def _str_en_zh(self,str):
        """
        私有方法：判断字符串是否同时包含英文和中文
        @param str  被查字符串
        @return res 返回结果，如果是则返回true，否则返回false
        """
        res = True
        i = 0
        for ch in str:
            if  u'\u4e00' <= ch <= u'\u9fa5':
                i +=1
                break
        for en in str:
            if re.match(r'[a-z]',en) or re.match(r'[A-Z]',en):
                i +=1
                break
        if i == 2:
            res = True
        else:
            res = False
        return res

def main():
    file_in = './dataset/movies.csv'
    file_out = './data/process/movie.csv'
    n = DataMovie(file_in,file_out)
    n.data_process()

if __name__ == '__main__':
    main()