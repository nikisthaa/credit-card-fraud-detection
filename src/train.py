
import os
import sys
import shutil
import logging
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from dotenv import load_dotenv

from constants import BUCKET_NAME, TEMP_MODEL_PATH
from utils import get_spark, upload

load_dotenv()

def train_model(df):
    train, test = df.randomSplit([0.7, 0.3], seed = 2018)

    rf_clf = RandomForestClassifier(featuresCol = 'scaledFeatures', labelCol = 'Class')
    rfModel = rf_clf.fit(train)

    predictions = rfModel.transform(test)

    evaluator = MulticlassClassificationEvaluator(labelCol="Class", predictionCol="prediction")

    accuracy = evaluator.evaluate(predictions)

    print("Training Piepline: Training of Model completed. Overal accuracy is {0}".format(accuracy))

    rfModel.write().overwrite().save(TEMP_MODEL_PATH)
    
    upload(BUCKET_NAME, TEMP_MODEL_PATH, "model")

    return accuracy

def main():
    spark = get_spark()
    # print(os.path.join(os.environ["HDFS_FILE_PATH"],sys.argv[1]))
    preprocessed_df = spark.read.parquet(os.path.join(os.environ["HDFS_FILE_PATH"],sys.argv[1]))
    train_model(preprocessed_df)

main()
