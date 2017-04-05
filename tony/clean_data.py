# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from collections import Counter

# 定义文件名
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

num_sample = 1000

file_list = [ACTION_201602_FILE, ACTION_201603_FILE, ACTION_201603_EXTRA_FILE, 
ACTION_201604_FILE, USER_FILE, COMMENT_FILE]

for fname in file_list:
	with open("../data_ori/" + fname[8:], 'wb') as fw:
		with open(fname) as fr:
			for i in xrange(num_sample):
				fw.write(fr.readline())

