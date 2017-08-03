'''
import csv

description = []
with open("df_weibo_src_neg_test.csv",newline='') as fp:
    spamreader = csv.reader(fp, delimiter=' ', quotechar='|')
    for row in spamreader:
        #print(', '.join(row))
        description.append(row[0].split(',')[3])
        pass

'''

import pymysql
import jieba
import copy
import csv

db_user = 'industry'
db_pass = '123456industry'
db_host = '192.168.2.6'
db_port = '3306'
db_name = 'dingfu_industry'


connect = pymysql.connect(user=db_user, passwd=db_pass, db=db_name, host=db_host, charset="utf8",
                          use_unicode=True)
cursor = connect.cursor()
# uid,screen_name,gender,verified_reason,follow_cnt,followers_cnt,statuses_cnt
cursor.execute("SELECT * FROM df_weibo_src_neg_test")
data = cursor.fetchall()

jieba.load_userdict("dict.txt")
STOP_WORDS = [line.rstrip() for line in open("stop.txt","r")]




dict = {
    'word':[],
    'word_cnt':[]
}

pos_train={
    'verified_reason':[],
    'gender':[],
    'follow_cnt':[],
    'followers_cnt':[],
    'status_cnt':[],
    'financial':[]
}
pos_test=copy.deepcopy(pos_train)
neg_train=copy.deepcopy(pos_train)
neg_test=copy.deepcopy(pos_train)

for i in range(len(data)):
    line=data[i]
    default_mode = jieba.cut(line[3].replace('\n',''),cut_all=True)
    if i%3==0:
        neg_test['verified_reason'].append("/".join(default_mode))
        neg_test['gender'].append(line[4])
        neg_test['follow_cnt'].append(line[5])
        neg_test['followers_cnt'].append(line[6])
        neg_test['status_cnt'].append(line[7])
        neg_test['financial'].append(False)
    else:
        #neg_train.append(t.copy())
        neg_train['verified_reason'].append("/".join(default_mode))
        neg_train['gender'].append(line[4])
        neg_train['follow_cnt'].append(line[5])
        neg_train['followers_cnt'].append(line[6])
        neg_train['status_cnt'].append(line[7])
        neg_train['financial'].append(False)


keys = sorted(neg_train.keys())
with open('train.csv','w+') as fp:
    writer = csv.writer(fp)
    writer.writerow(keys)
    writer.writerows(zip(*[neg_train[key] for key in keys]))

with open('test.csv','w+') as fp:
    writer = csv.writer(fp)
    writer.writerow(keys)
    writer.writerows(zip(*[neg_test[key] for key in keys]))
    
for line in data:
    words = '/'.join(jieba.cut(line[3].replace('\n',''),cut_all=True)).split('/')
    for word in words:
        if len(word)>0:
            if word not in dict['word']:
                dict['word'].append(word)
                dict['word_cnt'].append(1)
            else:
                t=dict['word'].index(word)
                dict['word_cnt'][dict['word'].index(word)] += 1


cursor.execute("SELECT * FROM df_weibo_src_pos_test")
data = cursor.fetchall()

for i in range(len(data)):
    line=data[i]
    default_mode = jieba.cut(line[3].replace('\n',''),cut_all=True)
    if i%3==0:
        pos_test['verified_reason'].append("/".join(default_mode))
        pos_test['gender'].append(line[4])
        pos_test['follow_cnt'].append(line[5])
        pos_test['followers_cnt'].append(line[6])
        pos_test['status_cnt'].append(line[7])
        pos_test['financial'].append(True)
    else:
        #pos_train.append(t.copy())
        pos_train['verified_reason'].append("/".join(default_mode))
        pos_train['gender'].append(line[4])
        pos_train['follow_cnt'].append(line[5])
        pos_train['followers_cnt'].append(line[6])
        pos_train['status_cnt'].append(line[7])
        pos_train['financial'].append(True)



with open('train.csv','a+') as fp:
    writer = csv.writer(fp)
    writer.writerows(zip(*[pos_train[key] for key in keys]))

with open('test.csv','a+') as fp:
    writer = csv.writer(fp)
    writer.writerows(zip(*[pos_test[key] for key in keys]))

for line in data:
    words = '/'.join(jieba.cut(line[3].replace('\n',''),cut_all=True)).split('/')
    for word in words:
        if len(word)>0:
            if word not in dict['word']:
                dict['word'].append(word)
                dict['word_cnt'].append(1)
            else:
                dict['word_cnt'][dict['word'].index(word)] += 1

keys = sorted(dict.keys())
with open('words.csv','w+') as fp:
    writer = csv.writer(fp)
    writer.writerow(keys)
    for i in range(len(dict['word'])):#range(100):#
        if dict['word_cnt'][i]>2 and len(dict['word'][i])>1:
            writer.writerow(dict[key][i] for key in keys)