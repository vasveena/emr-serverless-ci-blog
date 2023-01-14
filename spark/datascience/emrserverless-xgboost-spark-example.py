import pickle
from skdist.distribute.search import DistGridSearchCV
from sklearn.datasets import load_breast_cancer, load_boston
from xgboost import XGBClassifier, XGBRegressor
from pyspark.sql import SparkSession
from sklearn.datasets import make_classification
from joblib import logger

# spark session initialization
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

### XGBClassifier ###

# sklearn variables
cv = 5
clf_scoring = "roc_auc"
reg_scoring = "neg_mean_squared_error"

XX=make_classification(n_samples=100000, n_features=50)

X = XX[0]
y = XX[1]

grid = dict(
    learning_rate=[0.05, 0.01],
    max_depth=[4, 6, 8],
    colsample_bytree=[0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
    n_estimators=[100, 200, 300],
)

### distributed grid search
model = DistGridSearchCV(XGBClassifier(), grid, sc, cv=cv, scoring=clf_scoring, verbose=10)
# distributed fitting with spark
model.fit(X, y)
# predictions on the driver
preds = model.predict(X)
probs = model.predict_proba(X)


print("-- Grid Search --")
print("Best Score: {0}".format(model.best_score_))
print("Best colsample_bytree: {0}".format(model.best_estimator_.colsample_bytree))
print("Best learning_rate: {0}".format(model.best_estimator_.learning_rate))
print("Best max_depth: {0}".format(model.best_estimator_.max_depth))
print("Best n_estimators: {0}".format(model.best_estimator_.n_estimators))
