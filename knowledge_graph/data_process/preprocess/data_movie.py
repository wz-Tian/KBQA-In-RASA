"""
对movie所有相关数据进行数据预处理
@author ZhouZL<zhouzl_2002@163.com>
@date 2023.08.26
@version 1.1.0
"""
# -*- coding: utf-8 -*-
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
            'movie_id',#电影ID
            'movie_name',#名称
            'alias',#别名
            'storyline',#电影情节描述
            'mins',#电影时长
            'tag',#标签
            'category',#类型
            'regions',#地区
            'languages',#电影使用语言
            'year',#上映日期
            'actor_list',#演员与PERSON_ID的对应关系,多个演员采用“\|”符号分割，格式“演员A:ID\|演员B:ID”
            'director_list',#导演与PERSON_ID的对应关系,多个导演采用“\|”符号分割，格式“导演A:ID\|导演B:ID”；
            'score',#豆瓣评分
            'votes',#豆瓣投票数
        )
        cls.list_col_in = (
            'MOVIE_ID',#电影ID
            'NAME',#名称
            'ALIAS',#别名
            'STORYLINE',#电影情节描述
            'MINS',#电影时长
            'TAGS',#标签
            'GENRES',#类型
            'REGIONS',#地区
            'LANGUAGES',#电影使用语言
            'RELEASE_DATE',#上映日期
            'ACTOR_IDS',#演员与PERSON_ID的对应关系,多个演员采用“\|”符号分割，格式“演员A:ID\|演员B:ID”
            'DIRECTOR_IDS',#导演与PERSON_ID的对应关系,多个导演采用“\|”符号分割，格式“导演A:ID\|导演B:ID”；
            'DOUBAN_SCORE',#豆瓣评分
            'DOUBAN_VOTES',#豆瓣投票数
        )
        cls.list_col_delete = (
            'COVER',#封面图片地址
            'YEAR',#上映年份
            'IMDB_ID',#IMDbID
            'ACTORS',#主演
            'OFFICIAL_SITE',#官网地址
            'DIRECTORS',#导演
            'SLUG',#加密的url，可忽略
        )
    
    def data_read(self):
        """
        从CSV文件中读入数据
        """
        self.data = pd.read_csv(self.path_dataset,encoding='utf-8',dtype=str)
        self.data = self.data.drop_duplicates(subset=['MOVIE_ID'])
        self.data = self.data.sort_values(by="MOVIE_ID", ascending=True)
        # self.data = pd.read_csv(self.path_dataset, encoding='utf-8', dtype=str)
    
    def data_deal(self):
        """
        对读入的数据进行调整
        """
        #删除用不到的列
        # for col in self.list_col_delete:
        #     self.data = self.data.drop(col,axis=1)

        #对电影数据的空值称行处理
        # self.data = self.data.dropna()
        # print(self.data)
        # self.data = self.data.dropna(subset=['MOVIE_ID', 'NAME','DOUBAN_SCORE','DOUBAN_VOTES'])
        # self.data = self.data.fillna(
        #     {
        #     'ALIAS': '无',
        #     'STORYLINE': '无',
        #     'MINS': '未知',
        #     'TAGS': '无',
        #     'GENRES': '无',
        #     'REGIONS': '未知',
        #     'LANGUAGES': '未知',
        #     'RELEASE_DATE': '未知',
        #     'ACTOR_IDS':'未知',
        #     'DIRECTOR_IDS':'未知'
        #     }
        # )
        # 对部分数据单独处理
        for row in range(self.data.shape[0]):
            #对电影上映年份信息进行单独截取
            self.data.loc[row,'RELEASE_DATE'] = self.data.loc[row,'RELEASE_DATE'].split('-')[0]
            #去除类别、语言、地区、标签之中的空格
            self.data.loc[row,'NAME'] = self.data.loc[row,'NAME'].replace(' ','')
            self.data.loc[row,'ALIAS'] = self.data.loc[row,'ALIAS'].replace(' ','')
            self.data.loc[row,'GENRES'] = self.data.loc[row,'GENRES'].replace(' ','')
            self.data.loc[row,'LANGUAGES'] = self.data.loc[row,'LANGUAGES'].replace(' ','')
            self.data.loc[row,'TAGS'] = self.data.loc[row,'TAGS'].replace(' ','')
            self.data.loc[row,'REGIONS'] = self.data.loc[row,'REGIONS'].replace(' ','')
            #去掉所有列中的分隔符
            self.data.loc[row,'NAME'] = self.data.loc[row,'NAME'].replace('·','')
            self.data.loc[row,'ALIAS'] = self.data.loc[row,'ALIAS'].replace('·','')
            self.data.loc[row,'STORYLINE'] = self.data.loc[row,'STORYLINE'].replace('·','')
            self.data.loc[row,'LANGUAGES'] = self.data.loc[row,'LANGUAGES'].replace('·','')
            self.data.loc[row,'TAGS'] = self.data.loc[row,'TAGS'].replace('·','')
            self.data.loc[row,'GENRES'] = self.data.loc[row,'GENRES'].replace('·','')
            self.data.loc[row,'REGIONS'] = self.data.loc[row,'REGIONS'].replace('·','')
            self.data.loc[row,'ACTOR_IDS'] = self.data.loc[row,'ACTOR_IDS'].replace('·','')
            self.data.loc[row,'DIRECTOR_IDS'] = self.data.loc[row,'DIRECTOR_IDS'].replace('·','')
            #删除演员列表中没有ID的演员
            if not self.data.loc[row,'ACTOR_IDS'] == '未知':
                self.data.loc[row,'ACTOR_IDS'] = self.data.loc[row,'ACTOR_IDS'].replace(' ','')
                self.data.loc[row,'ACTOR_IDS'] = self.data.loc[row,'ACTOR_IDS'].replace(':','：')
                list = self.data.loc[row,'ACTOR_IDS'].split('|')
                str = ''
                strList = []
                for substr in list:
                    if  len(substr.split('：')) == 1 or substr.split('：')[1] == '':
                        list.remove(substr)
                if len(list) == 0:
                    list.append('未知')
                    strList.append(list[0])
                else:
                    strList.append(list[0])
                    for item in list[1:]:
                        strList.append(item)
                self.data.loc[row,'ACTOR_IDS'] = '|'.join(strList)
            #删除导演列表中没有ID的导演
            if not self.data.loc[row,'DIRECTOR_IDS'] == '未知':
                self.data.loc[row,'DIRECTOR_IDS'] = self.data.loc[row,'DIRECTOR_IDS'].replace(' ','')
                self.data.loc[row,'DIRECTOR_IDS'] = self.data.loc[row,'DIRECTOR_IDS'].replace(':','：')
                list = self.data.loc[row,'DIRECTOR_IDS'].split('|')
                str = ''
                strList = []
                for substr in list:
                    if  len(substr.split('：')) == 1 or substr.split('：')[1] == '':
                        list.remove(substr)
                if len(list) == 0:
                    list.append('未知')
                    strList.append(list[0])
                else:
                    strList.append(list[0])
                    for item in list[1:]:
                        strList.append(item)
                self.data.loc[row,'DIRECTOR_IDS'] = '|'.join(strList)

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

def main():
    file_in = './dataset/movies_3.csv'
    file_out = './data/process/movieInfo2.csv'
    n = DataMovie(file_in,file_out)
    n.data_process()

if __name__ == '__main__':
    main()