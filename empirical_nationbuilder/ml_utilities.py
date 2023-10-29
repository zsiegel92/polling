from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import pyspark.sql.functions as F
from pyspark.ml.classification import MultilayerPerceptronClassifier

from spark_utilities import get_table, df_subset, printSchema, sample_pandas, cast_cols_date,cast_cols, cast_cols_year, cast_cols_categorical_onehot, cast_numeric_binary,get_feature_vector_col, drop_column, replace_empty_with_na, drop_na,replace_na_with_default, rename_column, add_column_to_constant, multiply_column_by_constant,run_query_on_tmp



# https://spark.apache.org/docs/latest/ml-classification-regression.html#multilayer-perceptron-classifier
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.classification.MultilayerPerceptronClassifier.html#pyspark.ml.classification.MultilayerPerceptronClassifier
# "Number of inputs has to be equal to the size of feature vectors. Number of outputs has to be equal to the total number of labels."
def get_mlp(lrdf,featureOutputCol='features', labelCol = 'label', hiddenLayers = [40,40]):
	pdf = sample_pandas(lrdf,nrows=10,printdf=False)
	n_input = pdf[featureOutputCol][0].size
	n_output = 2 # for 2 categories - not 1! Uses one-hot encoding. # pdf[labelCol][0].size
	layers = [n_input] + hiddenLayers + [n_output]
	trainer = MultilayerPerceptronClassifier(maxIter=100, featuresCol=featureOutputCol, labelCol=labelCol,layers=layers, seed=1234)
	print("ABOUT TO START TRAINING MLP")
	nnModel = trainer.fit(lrdf)
	print("TRAINED MLP")
	return nnModel


def get_lrModel(lrdf, featureOutputCol='features', labelCol = 'label'):
	lr = LogisticRegression(featuresCol=featureOutputCol, labelCol=labelCol, maxIter=10)
	lrModel = lr.fit(lrdf)
	return lrModel
