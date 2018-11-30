"""
Created on Sat Dec 01 2018
@author: JeongChanwoo
"""
from surprise.model_selection import cross_validate, KFold
from surprise import Dataset,Reader
from surprise import SVD
from surprise import accuracy
from collections import defaultdict
from tqdm import tqdm
from preprocess import DataReader
from evaluate import precision_recall_at_k
import pickle
from itertools import repeat
import pandas as pd
import numpy as np
import time

def surprise_data_transform(data , scale_range = (0,1) , eval = True):
    reader = Reader(rating_scale = scale_range)
    data = Dataset.load_from_df(data[['userID', 'itemID', 'rating']], reader)
    if eval = True:
        return data
    elif eval = False:
        train_set = data.build_full_trainset()
        return train_set

def recommend_model_testing(data, fold = 5, top_k = 10, n_factors = 200, n_epochs = 30, threshold = 1):
    model_algo = SVD(n_factors=200, n_epochs=30)
    np.random.seed(42)

    acc = np.zeros(fold)
    recall_n = np.zeros(fold)
    precision_n = np.zeros(fold)

    cv = KFold(fold)
    for i, (train_set, test_set) in tqdm(enumerate(cv.split(data))):
        model_algo.fit(trainset=train_set)
        predictions = svd_algo.test(testset=test_set)


        acc[i] = surprise.accuracy.rmse(predictions=predictions, verbose=True)
        precisions, recalls  = precision_recall_at_k(predictions, k=top_k, threshold = threshold)
        total_precisions = sum(prec for prec in precisions.values()) / len(precisions)
        total_recalls = sum(rec for rec in recalls.values()) / len(recalls)
        recall_n[i] = total_recalls
        precision_n[i] = total_precisions

    total_score = np.vstack((acc,recall_n, precision_n )).T
    total_score = pd.DataFrame(total_score, columns=['acc' , 'recall_' + str(top_k), 'precision_' + str(top_k) ])
    return svd_algo, total_score

def recommend_model(model,train_set):
    model.train(trainset)
    return model

def get_top_n(predictions, n=10):
    top_n = defaultdict(list)
    for uid, iid, true_r, est, _ in predictions:
        top_n[uid].append((iid, lecture_data.loc[iid]['item_name'], est))

    for uid, user_ratings in top_n.items():
        user_ratings.sort(key = lambda x : x[2],  reverse = True)
        top_n[uid] = user_ratings[:n]

    return top_n

def recommend_predict(model,negative_set):
    predictions = model.test(negative_set)
    return predictions

def recommend_file_save(recommend_data, file_name):
    with open(file_name, 'wb') as rec_save:
        pickle.dump(recommend_data, rec_save, protocol=pickle.HIGHEST_PROTOCOL)

def model_save(model, model_name):
    with open(model_name, 'wb') as model_save:
        pickle.dump(model, model_save, protocol=pickle.HIGHEST_PROTOCOL)

def model_score_save(score_data, file_name):
    with open(file_name , 'a') as file_save:
        pickle.dump(score_data, file_save, protocol=pickle.HIGHEST_PROTOCOL)

if __name__ == '__main__':
    ### compile dict
    #### this section will be rebased to args file
    preprocess_dict = {
    'data_path' : '../',
    'user_cut_off' : 20,
    'lecture_cut_off' : 10,
    'activ_value' : 1
    }

    model_dict = {
    'scale_range' : (0,1),
    'fold' : 5,
    'top_k' : 10,
    'n_factors' : 200,
    'n_epochs' : 30,
    'threshold' : 1
    }

    saving_dict ={
    'path':'../',
    'model_name' : 'recommend_model_SVD'
    'recommend_file_name' : 'recommending_file'
    'recommend_score_name' : 'recommend_score'
    }
    ### modeling sector
    data_generator = DataReader()
    data_generator.load_data(preprocess_dict['data_path'])
    data = data_generator.data_preprocess(
                                    user_cut_off = preprocess_dict['user_cut_off'],
                                    lecture_cut_off =preprocess_dict['lecture_cut_off'],
                                    active_value = preprocess_dict[activ_value''] )
    lecture_data = data_generator.lecture_data

    print("model testing start!!!!!!")
    crosstesting_data = surprise_data_transform(data,
                                                scale_range = model_dict['scale_range'],
                                                eval = True)
    svd_algo ,total_score =recommend_model_testing(crosstesting_data ,
                                                fold = model_dict['fold'],
                                                top_k = model_dict['top_k'],
                                                n_factors = model_dict['n_factors'],
                                                n_epochs = model_dict['n_epochs'],
                                                threshold = model_dict['threshold'])
    print(total_score)

    train_set = surprise_data_transform(data ,
                                        scale_range = model_dict['scale_range'] ,
                                        eval = False)
    svd_algo = recommend_model(svd_algo, train_set)

    ### recommending sector
    predictions = recommend_predict(svd_algo, train_set.build_anti_testset())
    top_k = get_top_n(predictions, n = model_dict['top_k'])

    ### save score, model and recommed dict sector
    time = time.strftime('%x', time.localtime(time.time()))
    model_name = "{0}_{1}".format(saving_dict['model_name'], time)
    file_name =  "{0}_{1}_{2}".format(saving_dict['recommend_file_name'],model_dict[top_k] time)
    score_name =  "{0}".format(saving_dict['recommend_score_name'])

    time_list = list(repeat(time, model_dict['fold']))
    model_list = list(repeat(model_name, model_dict['fold']))
    date_info = pd.DataFrame(time_list, columns= ['Date'])
    model_info = pd.DataFrame(model_list, columns= ['Model'])

    total_score = pd.concat(['date_info', 'model_info','total_score' ] ,axis =1)
    model_score_save(total_score,score_name )
    recommend_file_save(top_k , file_name)
    model_save(svd_algo, model_name)
