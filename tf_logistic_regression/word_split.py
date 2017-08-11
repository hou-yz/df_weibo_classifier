
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

jieba.load_userdict("dict.txt")
STOP_WORDS = [line.rstrip() for line in open("stop.txt","r")]

neg,pos,nv=1,1,0
#not verified

predict=1

word_update=1

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
follower_test = copy.deepcopy(pos_train)
follower_train = copy.deepcopy(pos_train)

keys = sorted(neg_train.keys())

if neg:

    cursor.execute("SELECT * FROM df_weibo_src_neg_test")
    data = cursor.fetchall()
    for i in range(len(data)):
        line = data[i]
        if line[3] != '-1':
            vr = "/".join(jieba.cut(line[3].replace('\n', ''), cut_all=True))
        else:
            vr = "NULL"

        if '/' not in vr:
            vr = '/' + vr
        words = vr.split('/')
        for word in words:
            if len(word) > 0 and word not in STOP_WORDS:
                if word not in dict['word']:
                    dict['word'].append(word)
                    dict['word_cnt'].append(1)
                else:
                    dict['word_cnt'][dict['word'].index(word)] += 1

        if i % 3 == 0:
            neg_test['verified_reason'].append(vr)
            neg_test['gender'].append(line[4])
            neg_test['follow_cnt'].append(line[5])
            neg_test['followers_cnt'].append(line[6])
            neg_test['status_cnt'].append(line[7])
            neg_test['financial'].append(0)  # 'V_Else'
        else:
            # neg_train.append(t.copy())
            neg_train['verified_reason'].append(vr)
            neg_train['gender'].append(line[4])
            neg_train['follow_cnt'].append(line[5])
            neg_train['followers_cnt'].append(line[6])
            neg_train['status_cnt'].append(line[7])
            neg_train['financial'].append(0)

    with open('train.csv', 'w+') as fp:
        writer = csv.writer(fp)
        writer.writerow(keys)
        writer.writerows(zip(*[neg_train[key] for key in keys]))

    with open('test.csv', 'w+') as fp:
        writer = csv.writer(fp)
        writer.writerow(keys)
        writer.writerows(zip(*[neg_test[key] for key in keys]))

if pos:

    cursor.execute("SELECT * FROM df_weibo_src_pos_test")
    data = cursor.fetchall()

    for i in range(len(data)):
        line = data[i]
        if line[3] != '-1':
            vr = "/".join(jieba.cut(line[3].replace('\n', ''), cut_all=True))
        else:
            vr = "NULL"

        if '/' not in vr:
            vr = '/' + vr
        words = vr.split('/')
        for word in words:
            if len(word) > 0 and word not in STOP_WORDS:
                if word not in dict['word']:
                    dict['word'].append(word)
                    dict['word_cnt'].append(1)
                else:
                    dict['word_cnt'][dict['word'].index(word)] += 1

        if i % 3 == 0:
            pos_test['verified_reason'].append(vr)
            pos_test['gender'].append(line[4])
            pos_test['follow_cnt'].append(line[5])
            pos_test['followers_cnt'].append(line[6])
            pos_test['status_cnt'].append(line[7])
            pos_test['financial'].append(1)  # 'V_Financial'
        else:
            # pos_train.append(t.copy())
            pos_train['verified_reason'].append(vr)
            pos_train['gender'].append(line[4])
            pos_train['follow_cnt'].append(line[5])
            pos_train['followers_cnt'].append(line[6])
            pos_train['status_cnt'].append(line[7])
            pos_train['financial'].append(1)

    with open('train.csv', 'a+') as fp:
        writer = csv.writer(fp)
        writer.writerows(zip(*[pos_train[key] for key in keys]))

    with open('test.csv', 'a+') as fp:
        writer = csv.writer(fp)
        writer.writerows(zip(*[pos_test[key] for key in keys]))

if nv:

    cursor.execute("SELECT * FROM df_weibo_src_follower_test LIMIT 1000")
    data = cursor.fetchall()

    for i in range(len(data)):
        line = data[i]
        if line[3] != '-1':
            vr = "/".join(jieba.cut(line[3].replace('\n', ''), cut_all=True))
        else:
            vr = "NULL"

        if '/' not in vr:
            vr = '/' + vr
        words = vr.split('/')
        for word in words:
            if len(word) > 0 and word not in STOP_WORDS:
                if word not in dict['word']:
                    dict['word'].append(word)
                    dict['word_cnt'].append(1)
                else:
                    dict['word_cnt'][dict['word'].index(word)] += 1

        if i % 3 == 0:
            follower_test['verified_reason'].append(vr)
            follower_test['gender'].append(line[4])
            follower_test['follow_cnt'].append(line[5])
            follower_test['followers_cnt'].append(line[6])
            follower_test['status_cnt'].append(line[7])
            follower_test['financial'].append(2)  # 'NV'
        else:
            # follower_train.append(t.copy())
            follower_train['verified_reason'].append(vr)
            follower_train['gender'].append(line[4])
            follower_train['follow_cnt'].append(line[5])
            follower_train['followers_cnt'].append(line[6])
            follower_train['status_cnt'].append(line[7])
            follower_train['financial'].append(2)

    with open('train.csv', 'a+') as fp:
        writer = csv.writer(fp)
        writer.writerows(zip(*[follower_train[key] for key in keys]))

    with open('test.csv', 'a+') as fp:
        writer = csv.writer(fp)
        writer.writerows(zip(*[follower_test[key] for key in keys]))

if predict:

    cursor.execute("SELECT * FROM df_weibo_src_follower_test LIMIT 100000")
    data = cursor.fetchall()

    for i in range(len(data)):
        line = data[i]
        if line[3] != '-1':
            vr = "/".join(jieba.cut(line[3].replace('\n', ''), cut_all=True))
        else:
            vr = "NULL"

        if '/' not in vr:
            vr = '/' + vr
        words = vr.split('/')
        for word in words:
            if len(word) > 0 and word not in STOP_WORDS:
                if word not in dict['word']:
                    dict['word'].append(word)
                    dict['word_cnt'].append(1)
                else:
                    dict['word_cnt'][dict['word'].index(word)] += 1

        follower_test['verified_reason'].append(vr)
        follower_test['gender'].append(line[4])
        follower_test['follow_cnt'].append(line[5])
        follower_test['followers_cnt'].append(line[6])
        follower_test['status_cnt'].append(line[7])
        follower_test['financial'].append(2)  # 'NV'


    with open('predict.csv', 'w+') as fp:
        writer = csv.writer(fp)
        writer.writerows(zip(*[follower_test[key] for key in keys]))


if word_update:

    keys = sorted(dict.keys())
    with open('words.csv', 'w+') as fp:
        writer = csv.writer(fp)
        writer.writerow(keys)
        for i in range(len(dict['word'])):  # range(100):#
            if dict['word_cnt'][i] > 2 and len(dict['word'][i]) > 1:
                writer.writerow(dict[key][i] for key in keys)