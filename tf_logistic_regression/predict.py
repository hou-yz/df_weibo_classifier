import pandas as pd
import tempfile


COLUMNS = ["financial","follow_cnt","followers_cnt","gender","status_cnt","verified_reason",]

df_predict = pd.read_csv("predict.csv",names=COLUMNS, skipinitialspace=True,skiprows=0,encoding='utf-8')
words = pd.read_csv("words.csv",names=["word","word_cnt"],skipinitialspace=True,skiprows=1,encoding='utf-8')


CATEGORICAL_COLUMNS = ["gender"]
CONTINUOUS_COLUMNS = ["follow_cnt","followers_cnt","status_cnt",]
WORD_COLUMNS = "verified_reason"
LABEL_COLUMN = "financial"

import tensorflow as tf

tf.logging.set_verbosity(tf.logging.INFO)

def input_fn(df):
    # Creates a dictionary mapping from each continuous feature column name (k) to
    # the values of that column stored in a constant Tensor.
    continuous_cols = {k: tf.constant(df[k].values,dtype=tf.float32)
                       for k in CONTINUOUS_COLUMNS}
    #normalize
    #continuous_cols = {k: tf.nn.l2_normalize(continuous_cols[k],[0]) for k in CONTINUOUS_COLUMNS}

    # Creates a dictionary mapping from each categorical feature column name (k)
    # to the values of that column stored in a tf.SparseTensor.
    categorical_cols = {k: tf.SparseTensor(
        indices=[[i, 0] for i in range(df[k].size)],
        values=df[k].values,
        dense_shape=[df[k].size, 1])
                        for k in CATEGORICAL_COLUMNS}

    word_cols = {"word_%d" % k: tf.constant([(words["word"][k] in df["verified_reason"][i].split('/')) for i in range(df["verified_reason"].size)])
                        for k in range(len(words['word']))}

    #print(tf.Session().run(word_cols))

    # Merges the two dictionaries into one.
    feature_cols = dict(continuous_cols.items() | categorical_cols.items() | word_cols.items())#
    # Converts the label column into a constant Tensor.
    #label = tf.constant(df[LABEL_COLUMN].values)
    # Returns the feature columns and the label.
    return feature_cols, None

def predict_input_fn():
    return input_fn(df_predict)


gender = tf.contrib.layers.sparse_column_with_keys(column_name="gender", keys=["f", "m"])

follow_cnt = tf.contrib.layers.real_valued_column("follow_cnt")
followers_cnt = tf.contrib.layers.real_valued_column("followers_cnt")
status_cnt = tf.contrib.layers.real_valued_column("status_cnt")

verified_reason_words = [tf.contrib.layers.real_valued_column(column_name="word_%d" % k) for k in range(len(words['word']))]

total = [gender,follow_cnt,followers_cnt,status_cnt]+(verified_reason_words)#

wide_columns = verified_reason_words#[gender] +
deep_columns = [tf.contrib.layers.embedding_column(gender, dimension=10),
                follow_cnt,followers_cnt,status_cnt]#


model_dir = './model/2_classes_linear'#tempfile.mkdtemp()

m = tf.contrib.learn.LinearClassifier(feature_columns=wide_columns,
                                      optimizer=tf.train.FtrlOptimizer(
                                          learning_rate=0.1,
                                          l1_regularization_strength=1.0,
                                          l2_regularization_strength=1.0),
                                      model_dir=model_dir)
'''
m = tf.contrib.learn.DNNLinearCombinedClassifier(
    model_dir=model_dir,
    n_classes=3,
    linear_feature_columns=wide_columns,
    dnn_feature_columns=deep_columns,
    dnn_hidden_units=[50, 50, 20])
'''

result=list(m.predict_classes(input_fn=predict_input_fn))
with open('predict.txt','w') as fp:
    for line in result:
        fp.write(str(line)+'\n')


