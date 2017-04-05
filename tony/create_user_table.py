#!/Users/tony/anaconda/bin/python
# -*- coding: utf-8 -*-
# @Date    : 2017-03-31 16:17:43
# @Author  : Tony (cz9x@qq.com)

import pandas as pd
import numpy as np
from collections import Counter

ACTION_201602_FILE = "../data/JData_Action_201602.csv"
ACTION_201603_FILE = "../data/JData_Action_201603.csv"
ACTION_201603_EXTRA_FILE = "../data/JData_Action_201603_extra.csv"
ACTION_201604_FILE = "../data/JData_Action_201604.csv"
COMMENT_FILE = "../data/JData_Comment.csv"
PRODUCT_FILE = "../data/JData_Product.csv"
USER_FILE = "../data/JData_User.csv"
NEW_USER_FILE = "../data/JData_User_New.csv"
USER_TABLE_FILE = "../data/user_table.csv"
ITEM_TABLE_FILE = "../data/item_table.csv"

def add_type_count(group):
	behavior_type = group.type.astype(int)

	type_cnt = Counter(behavior_type)
	# 1: 浏览 2: 加购 3: 删除
    # 4: 购买 5: 收藏 6: 点击
	group['browse_num'] = type_cnt[1]
	group['addcart_num'] = type_cnt[2]
	group['delcart_num'] = type_cnt[3]
	group['buy_num'] = type_cnt[4]
	group['favor_num'] = type_cnt[5]
	group['click_num'] = type_cnt[6]

	return group[['user_id', 'browse_num', 'addcart_num', 'delcart_num', 'buy_num', 'favor_num','click_num']]

def get_from_action_data(fname, chunk_size=500000):
	reader = pd.read_csv(fname, header=0, iterator=True)
	chunks = []
	loop = True
	while loop:
		try:
			chunk = reader.get_chunk(chunk_size)[['user_id', 'type']]
			chunks.append(chunk)
		except StopIteration:
			loop = False
			print "Iteration is stop"

	df_ac = pd.concat(chunks, ignore_index=True)
	df_ac = df_ac.groupby(['user_id'], as_index=False).apply(add_type_count)
	df_ac = df_ac.drop_duplicates('user_id')

	return df_ac


def merge_action_data():
	df_ac = []
	df_ac.append(get_from_action_data(fname=ACTION_201602_FILE))
	df_ac.append(get_from_action_data(fname=ACTION_201603_FILE))
	df_ac.append(get_from_action_data(fname=ACTION_201603_EXTRA_FILE))
	df_ac.append(get_from_action_data(fname=ACTION_201604_FILE))

	df_ac = pd.concat(df_ac, ignore_index=True)
	df_ac = df_ac.groupby(['user_id'], as_index=False).sum()

	df_ac['buy_addcart_ratio'] = df_ac['buy_num'] / df_ac['addcart_num']
	df_ac['buy_browse_ratio'] = df_ac['buy_num'] / df_ac['browse_num']
	df_ac['buy_click_ratio'] = df_ac['buy_num'] / df_ac['click_num']
	df_ac['buy_favor_ratio'] = df_ac['buy_num'] / df_ac['favor_num']

	df_ac.ix[df_ac['buy_addcart_ratio'] > 1., 'buy_addcart_ratio'] = 1.
	df_ac.ix[df_ac['buy_browse_ratio'] > 1., 'buy_browse_ratio'] = 1.
	df_ac.ix[df_ac['buy_click_ratio'] > 1., 'buy_click_ratio'] = 1.
	df_ac.ix[df_ac['buy_favor_ratio'] > 1., 'buy_favor_ratio'] = 1.

	return df_ac


def get_from_jdata_user():
    df_usr = pd.read_csv(USER_FILE, header=0)
    df_usr = df_usr[["user_id", "age", "sex", "user_lv_cd"]]
    return df_usr



user_base = get_from_jdata_user()
user_behavior = merge_action_data()
# 连接成一张表，类似于SQL的左连接(left join)
user_behavior = pd.merge(user_base, user_behavior, on=['user_id'], how='left')
# 保存为user_table.csv
user_behavior.to_csv(USER_TABLE_FILE, index=False)









