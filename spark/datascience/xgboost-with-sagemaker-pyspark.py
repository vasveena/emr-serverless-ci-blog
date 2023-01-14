import os
import io
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import boto3
import sagemaker
import sagemaker_pyspark
import random
from sagemaker_pyspark import IAMRole, S3DataPath
from sagemaker_pyspark.algorithms import XGBoostSageMakerEstimator
from pyspark.sql.types import DoubleType
import matplotlib.pyplot as plt
import numpy as np
from sagemaker_pyspark import SageMakerResourceCleanup

print(len(sys.argv))
if (len(sys.argv) != 5):
   print("Usage: emrserverless-datascience-with-ci [Sagemaker IAM role] [S3 bucket name] [S3 prefix name] [AWS region]")
   sys.exit(0)

role = sys.argv[1]
bucket = sys.argv[2]
prefix = sys.argv[3]
region = sys.argv[4]

spark = SparkSession\
        .builder\
        .appName("XGBoost application on MNIST dataset")\
        .enableHiveSupport()\
        .getOrCreate()

trainingData = (
    spark.read.format("libsvm")
    .option("numFeatures", "784")
    .option("vectorType", "dense")
    .load("s3a://sagemaker-sample-data-{}/spark/mnist/train/".format(region))
)

testData = (
    spark.read.format("libsvm")
    .option("numFeatures", "784")
    .option("vectorType", "dense")
    .load("s3a://sagemaker-sample-data-{}/spark/mnist/test/".format(region))
)

trainingData.show()

xgboost_estimator = XGBoostSageMakerEstimator(
    sagemakerRole=IAMRole(role),
    trainingInstanceType="ml.m4.xlarge",
    trainingInstanceCount=1,
    endpointInstanceType="ml.m4.xlarge",
    endpointInitialInstanceCount=1,
)

xgboost_estimator.setEta(0.2)
xgboost_estimator.setGamma(4)
xgboost_estimator.setMinChildWeight(6)
xgboost_estimator.setSilent(0)
xgboost_estimator.setObjective("multi:softmax")
xgboost_estimator.setNumClasses(10)
xgboost_estimator.setNumRound(10)

# train
model = xgboost_estimator.fit(trainingData)

# inference

transformedData = model.transform(testData)

transformedData.show()

# helper function to display a digit
def show_digit(img, caption="", xlabel="", subplot=None):
    if subplot == None:
        _, (subplot) = plt.subplots(1, 1)
    imgr = img.reshape((28, 28))
    subplot.axes.get_xaxis().set_ticks([])
    subplot.axes.get_yaxis().set_ticks([])
    plt.title(caption)
    plt.xlabel(xlabel)
    subplot.imshow(imgr, cmap="gray")


images = np.array(transformedData.select("features").cache().take(250))
clusters = transformedData.select("prediction").cache().take(250)

for cluster in range(10):
    print("\n\n\nCluster {}:".format(int(cluster)))
    digits = [img for l, img in zip(clusters, images) if int(l.prediction) == cluster]
    height = ((len(digits) - 1) // 5) + 1
    width = 5
    plt.rcParams["figure.figsize"] = (width, height)
    _, subplots = plt.subplots(height, width)
    subplots = np.ndarray.flatten(subplots)
    for subplot, image in zip(subplots, digits):
        show_digit(image, subplot=subplot)
    for subplot in subplots[len(digits) :]:
        subplot.axis("off")
    #plt.show()
    img_data = io.BytesIO()
    plt.savefig(img_data, format='png')
    img_data.seek(0)
    client = boto3.client('s3')
    client.put_object(Body=img_data, ContentType='image/png', Bucket=bucket, Key=prefix)

resource_cleanup = SageMakerResourceCleanup(model.sagemakerClient)
resource_cleanup.deleteResources(model.getCreatedResources())
