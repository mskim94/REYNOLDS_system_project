"""
Created on Sat Dec 01 2018
@author: JeongChanwoo
"""
import pandas as pd
import numpy as np
import re
from os import listdir

class DataReader(object):
    def __init__(self):
        self.data_list = None
        self.total_data = None
        self.user_lecture_data = None
        self.user_data = None
        self.user_index = None
        self.lecture_index = None

    def data_list_rise(self,path):
        file_list = listdir(path)
        data_list = []
        for k in file_list:
            regural = re.compile('(unit_complete)|(start_course)|(unit_complete)')
            m = regural.search(k)
            if m is not None:
                data_list.append(k)

        self.data_list = data_list
        return data_list


    def read_json(self, data_list):
        data_dict = {}
        for k in data_list:
            try:
                data_dict[k[:-5]] = pd.read_json(path + k)

            except pd.errors.ParserError:
                data_dict[k[:-5]] = pd.read_json(path + k sep = '\t')

        return data_dict


    def total_merge(self, data_dict):
        total = None
        data_dict = read_json()

        for idx in sorted(list(data_dict.keys()):
            total = pd.concat([total, data[idx]], axis = 0)

        total.reset_index(inplace = True)
        total.drop(['index'], axis =1, inplace = True)

        self.total_data = total

        return total

    def load_data(self,path):
        data_list = self.data_list_rise(path)
        data_dict = self.read_json(data_list)
        self.total_merge(data_dict)
        print("data_load_complete!!!")
        print("=" * 100)
    def cut_off_index(self,user_cut_off,lecture_cut_off):
        '''
        임계치 이상 수강한 강좌,
        임계치 이상 수강한 유저 인덱스 ID 값 할당
        '''
        data = self.total_data[['강좌ID', '유저ID']]
        data = data.drop_duplicates(['강좌ID','유저ID'])

        user_lecture_count = data['유저ID'].value_counts().to_frame()
        lecture_user_count = data['강좌ID'].value_counts().to_frame()

        if user_cut_off is not None:
            self.user_index = user_lecture_count[user_lecture_count['유저ID'] >= user_cut_off].index

        elif user_cut_off is None:
            self.user_index = user_lecture_count.index

        if lecture_cut_off is not None:
            self.lecture_index = lecture_user_count[lecture_user_count['강좌ID'] >= lecture_cut_off].index

        elif lecture_cut_off is None:
            self.lecture_index = lecture_user_count.index()

    def data_preprocess(self, user_cut_off = None, lecture_cut_off = None, active_value = 1):
        '''
        임계치 값으로 전처리 시도함
        액티브 활동에 대해 1로 채워 바이너리 값으로 간주
        user_lecture_data 값이 실제 추천 모델에 들어가는 값이 됨
        '''
        self.cut_off_index(user_cut_off, lecture_cut_off)


        lecture_index = self.lecture_index
        user_index = self.user_index

        print(("user_number : {0} \n
                lecture_number : {1}").format(len(user_index), len(lecture_index)))


        ### 강좌 정보 테이블
        lecture_data = self.total_data[['강좌ID', '강좌명']].drop_duplicates()
        if lecture_index is not None:
            lecture_data = lecture_data.loc[lecture_data['강좌ID'].isin(lecture_index)]
        ### 강좌 칼럼 이름 변경
        lecture_data.rename(columns={'강좌ID' : 'itemID',
                                                      '강좌명' : 'item_name'}, inplace = True)

        lecture_data.reset_index(inplace = True)
        lecture_data.drop(['index'],axis = 1, inplace=True)
        lecture_data.set_index('itemID', inplace=True)

        self.lecture_data = lecture_data

        ###유저 강좌 테이블
        user_lecture_data = self.total_data[['강좌ID', '유저ID']]
        if lecture_index is not None:
            user_lecture_data = user_lecture_data.loc[user_lecture_data['강좌ID'].isin(lecture_index)]

        if user_index is not None:
            user_lecture_data = user_lecture_data.loc[user_lecture_data['유저ID'].isin(user_index)]


        user_lecture_data = user_lecture_data.drop_duplicates(['강좌ID', '유저ID'])
        user_lecture_data['active'] = active_value
        user_lecture_data = user_lecture_data[(user_lecture_data.유저ID !=0)  &  (user_lecture_data.유저ID !=17)  ]


        ###

        ### 유저 강좌 칼럼 이름 변경
        user_lecture_data.rename(columns={'강좌ID' : 'itemID',
                                                      '유저ID' : 'userID',
                                                      'active' : 'rating'}, inplace = True)

        user_lecture_data = pd.DataFrame(user_lecture_data, columns=['itemID', 'userID', 'rating'])

        self.user_lecture_data = user_lecture_data
        print("Data set preprocessing is complete!!")
        print("=" * 100)

        return user_lecture_data
