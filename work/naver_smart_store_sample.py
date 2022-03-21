# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 22:10:16 2022

@author: user
"""

from pymongo import MongoClient
import pandas as pd
import numpy as np
import utility_work
import re

# 판매상품 50개 이하 / 카테고리 : 생활/건강, 식품, 패션잡화, 패션의류, 가구/인테리어


host = "49.50.173.55"
port = 27017
database = "naver_shopping"
client = MongoClient(host, port)
db = client.get_database(database)

match = '{' + ' "$and": [ '+ '{' + ' "prodCnt": ' + '{' + ' "$lte": 50 ' + '}' + '}, ' + '{' + ' "repCatNm": ' + '{' + ' "$in": ["생활/건강", "식품", "패션잡화", "패션의류", "가구/인테리어"] ' + '}' + '}' + '] ' + '}, ' + '{' + '"channel_id": 1, "mallName": 1, "prodCnt": 1, "mallGrade": 1, "keepCnt": 1, "repCatNm": 1, "businessType": 1, "identity": 1, "declaredToOnlineMarkettingNumber": 1, "businessAddressInfo": 1, "storeExposureInfo": 1 ' + '}'
project = '{' + '"channelNo": 1, "id": 1, "categoryId": 1, "categoryName": 1, "name": 1, "salePrice": 1, "discountedSalePrice": 1, "_averageReviewScore": 1, "_totalReviewCount": 1, "_productDateData": 1, "sellerTags": 1' + '}'
query = "db.smart_store_mall_info.find({match})".format(match=match)

smart_store_mall_sample = pd.DataFrame(eval(query))


#%% 판매상품이 50개 이하 / ["생활/건강", "식품", "패션잡화", "패션의류", "가구/인테리어"]

smart_store_mall_sample_BPOWER_premium = utility_work.get_isin(smart_store_mall_sample, "mallGrade", ["BPOWER", "PREMIUM"])
#smart_store_mall_sample.dropna(subset=['channel_id', 'identity'], inplace=True)
smart_store_mall_sample["channel_id"] = smart_store_mall_sample["channel_id"].astype(int).astype(str)
# smart_store_mall_sample_premium_5000 = utility_work.get_like(smart_store_mall_sample_premium, "channel_id", "5000")

smart_store_mall_sample_BPOWER_premium["repCatNm"].value_counts()

#%% 주소, 위경도, SNS

smart_store_mall_sample_BPOWER_premium.columns

address_list = []
lat_list = []
lon_list = []
naver_blog_list = []
insta_list = []
facebook_list = []
for i in range(len(smart_store_mall_sample_BPOWER_premium)):
    try:
        address = smart_store_mall_sample_BPOWER_premium["businessAddressInfo"][i]["fullAddressInfo"]
        address_list.append(address)
    except:
        address_list.append(np.nan)
        
    try:
        lat = smart_store_mall_sample_BPOWER_premium["businessAddressInfo"][i]["latitude"]
        lat_list.append(lat)
    except:
        lat_list.append(np.nan)

    try:
        lon = smart_store_mall_sample_BPOWER_premium["businessAddressInfo"][i]["longitude"]
        lon_list.append(lon)
    except:
        lon_list.append(np.nan)
    
    try:
        naver_blog = smart_store_mall_sample_BPOWER_premium["storeExposureInfo"][i]["exposureInfo"]["NAVERBLOG"][0]
        naver_blog_list.append(naver_blog)
    except:
        naver_blog_list.append(np.nan)
    
    try:        
        insta = smart_store_mall_sample_BPOWER_premium["storeExposureInfo"][i]["exposureInfo"]["INSTAGRAM"][0]
        insta_list.append(insta)
    except:
        insta_list.append(np.nan)
    
    try:
        facebook = smart_store_mall_sample_BPOWER_premium["storeExposureInfo"][i]["exposureInfo"]["FACEBOOK"][0]
        facebook_list.append(facebook)
    except:
        facebook_list.append(np.nan)

smart_store_mall_sample_BPOWER_premium["address"] = address_list
smart_store_mall_sample_BPOWER_premium["lat"] = lat_list
smart_store_mall_sample_BPOWER_premium["lon"] = lon_list
smart_store_mall_sample_BPOWER_premium["naver_blog"] = naver_blog_list
smart_store_mall_sample_BPOWER_premium["instagram"] = insta_list
smart_store_mall_sample_BPOWER_premium["facebook"] = facebook_list

smart_store_mall_sample_BPOWER_premium = smart_store_mall_sample_BPOWER_premium[['channel_id', 'mallName', 'prodCnt', 'mallGrade', 'keepCnt', 'repCatNm', 'businessType', 'identity', 'declaredToOnlineMarkettingNumber', 'address', 'lat', 'lon', 'naver_blog', 'instagram', 'facebook']]
#%% 카테고리별 200개 샘플에서 상품 데이터 생성

sample_mall = pd.DataFrame()
sample_products = pd.DataFrame()
no_data_lst = []
for category in smart_store_mall_sample_BPOWER_premium["repCatNm"].unique():
    
    print(category)
    
    # 해당 업종의 입점몰 
    cate_mall = utility_work.get_isin(smart_store_mall_sample_BPOWER_premium, "repCatNm", [category])
    
    # 샘플 200개 추출 & sample_mall DF 생성
    cate_mall = cate_mall.sample(n=200)
    sample_mall = pd.concat([sample_mall, cate_mall], ignore_index=False)
    
    # 입점몰 샘플 데이터의 몰ID로 상품 데이터 검색
    for i in range(len(cate_mall)):
        channel_id = str(cate_mall["channel_id"].iloc[i])
        #print(channel_id)
        seq = channel_id[-2:]
        #match = "{" + f"'channelNo':'{channel_id}'" + "}"
        match = '{' + ' "_productDateData.20220206.productDeliveryLeadTimes" : ' + '{' + ' "$exists" : True ' + '}, ' + f'"channelNo" : "{channel_id}" ' + '}'
        project = '{' + '"channelNo": 1, "id": 1, "categoryId": 1, "categoryName": 1, "name": 1, "salePrice": 1, "discountedSalePrice": 1, "_averageReviewScore": 1, "_totalReviewCount": 1, "_productDateData": 1, "sellerTags": 1' + '}'
        query = "db.smart_store_product_{seq}.find({match}, {project})".format(seq=seq, match=match, project=project)
        
        search_product = pd.DataFrame(eval(query))
        
        # 판매건수 데이터 산출 ( 최근 3일 판매건수 / 3 )
        try:
       
            recentSaleCount_lst = []
            for product_index in range(len(search_product["_productDateData"])):
                product = search_product["_productDateData"].iloc[product_index]
                
                try:
                    # 가장 최근날짜의 판매건수
                    recentSaleCount = product[list(product.keys())[-1]]["recentSaleCount"]
                    recentSaleCount_lst.append(recentSaleCount)
                except:
                    recentSaleCount_lst.append(np.nan)
                    
            search_product["recentSaleCount"] = recentSaleCount_lst
            sample_products = pd.concat([sample_products, search_product], ignore_index=True)

        except:
            no_data_lst.append(channel_id)
            print(f"No data === {channel_id}")

sample_mall["good_service"] = np.nan
sample_products = sample_products[['channelNo', 'id', '_averageReviewScore',  '_totalReviewCount', 'categoryId', 'categoryName', 'discountedSalePrice', 'name', 'salePrice', 'sellerTags', 'recentSaleCount']]

sample_products.rename(columns={"name":"product_nm"}, inplace=True)
#%% 카테고리 데이터 추가
            
samle_category = pd.DataFrame()
cate_1st = []
cate_2nd = []
cate_3rd = []
for cate_index in range(len(sample_products["categoryId"])):
    
    cate_id = sample_products["categoryId"][cate_index]
    match = '{' + f'"id":"{cate_id}"' + '}'
    project = '{' + '"id": 1, "chain_title": 1, "title": 1' + '}'
    query = "db.naver_shopping_category.find({match}, {project})".format(match=match, project=project)
            
    cate_df = pd.DataFrame(eval(query))
    
    print(f"{cate_df['title'][0]}")
    
    cate_classification = cate_df["chain_title"][0].split(" > ")
    cate_1st.append(cate_classification[0])
    cate_2nd.append(cate_classification[1])
    cate_3rd.append(cate_classification[2:])

samle_category["cate_1st"] = cate_1st
samle_category["cate_2nd"] = cate_2nd
samle_category["cate_3rd"] = cate_3rd


sample_mall_df = pd.DataFrame()
for i in range(len(sample_products)):
    print(sample_products["channelNo"][i])
    channelNo = sample_products["channelNo"][i]
    mall_data = utility_work.get_isin(sample_mall, "channel_id", [channelNo])
    sample_mall_df = pd.concat([sample_mall_df, mall_data], ignore_index=False)

sample_mall_df_a = sample_mall_df.reset_index().drop("index", axis=1)
#%%
mall_head = sample_mall_df.head()
products_head = sample_products.head()
category_head = samle_category.head()

#%%

sample_mall_df_5000 = sample_mall_df_a.iloc[:5000,:]
products_sample_5000 = sample_products.iloc[:5000,:]
samle_category_5000 = samle_category.iloc[:5000,:]

result = pd.concat([sample_mall_df_5000, products_sample_5000], axis=1)
result = pd.concat([result, samle_category_5000], axis=1)

#%%

result["salesCount"] = result['recentSaleCount'] / 3
result["salesCount"] = result["saleCount"].astype(int)
#%%



result.columns
result= result[['channel_id', 'mallName', 'prodCnt', 'mallGrade', 'good_service', 'keepCnt', 'repCatNm',
       'businessType', 'identity', 'declaredToOnlineMarkettingNumber',
       'address', 'lat', 'lon', 'naver_blog', 'instagram', 'facebook', 'id', 'categoryName', 'cate_1st', 'cate_2nd', 'cate_3rd', 'product_nm', 'salePrice', 'discountedSalePrice', '_averageReviewScore', '_totalReviewCount', 'salesCount', 'sellerTags']]

#%%
file_path = f"C:/Users/user/Desktop"
file_name = "sample_naver_shopping"

result.to_csv(f"{file_path}/{file_name}_utf.csv", index=False, sep=",", encoding="utf-8")
result = result.reset_index().drop("index", axis=1) 
result.to_excel(f"{file_path}/{file_name}.xlsx")

#%%

result.columns

file_path = f"C:/Users/user/Desktop"
file_name = "naver_smart_store_sample"
result = result.reset_index().drop("index", axis=1)   
result.to_excel(f"{file_path}/{file_name}.xlsx")

#%% 상품 조회

# df = utility_work.get_isin(smart_store_mall_sample_premium_5000, "repCatNm", ["생활/건강"])[:5]
# channel_id = str(df["channel_id"].iloc[1])
# print(channel_id)
# seq = channel_id[-2:]
# #match = "{" + f"'channelNo':'{channel_id}'" + "}"
# #match = '{' + ' "_productDateData.20220206.productDeliveryLeadTimes" : ' + '{' + ' "$exists" : True ' + '}, ' + f'"channelNo":"{channel_id}"' + '}'
# #match = '{' + ' "_productDateData.20220206.productDeliveryLeadTimes" : ' + '{' + ' "$exists" : True ' + '}' + ' }'
# match = '{' + ' "_productDateData.20220206.productDeliveryLeadTimes" : ' + '{' + ' "$exists" : True ' + '}, ' + f'"channelNo" : "{channel_id}" ' + '}'
# project = '{' + '"channelNo": 1, "id": 1, "categoryId": 1, "categoryName": 1, "name": 1, "salePrice": 1, "discountedSalePrice": 1, "_averageReviewScore": 1, "_totalReviewCount": 1, "_productDateData": 1, "sellerTags": 1' + '}'
# query = "db.smart_store_product_{seq}.find({match}, {project})".format(seq=seq, match=match, project=project)

# search_df = pd.DataFrame(eval(query))

# recentSaleCount_lst = []
# for product_index in range(len(search_df["_productDateData"])):
#     product = search_df["_productDateData"].iloc[product_index]
    
#     try:
#         recentSaleCount = product[list(product.keys())[-1]]["recentSaleCount"]
#         recentSaleCount_lst.append(recentSaleCount)
#     except:
#         recentSaleCount_lst.append(np.nan)

# search_df["recentSaleCount"] = recentSaleCount_lst

#%%

naver_smart_store_sample.columns 
naver_smart_store_sample["sales_cnt"] = (naver_smart_store_sample["recentSaleCount"] / 3).astype(int)

naver_smart_store_sample_update = naver_smart_store_sample[["channel_id", "mallName", 'prodCnt', "mallGrade", "keepCnt", "repCatNm", 'businessType', 'identity', 'declaredToOnlineMarkettingNumber', 'address', 'lat', 'lon', 'naver_blog', 'instagram', 'facebook', 'produc_id', 'categoryName', 'cate_1st', 'cate_2nd', 'cate_3rd', 'name', 'salePrice', 'discountedSalePrice', '_averageReviewScore', '_totalReviewCount', 'sales_cnt', 'sellerTags']] 
#%%

naver_sample