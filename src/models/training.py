import os
import pandas as pd
import documents as dc
import joblib
from dotenv import load_dotenv
from Extraction import Extraction
from PreProcessing import PreProcessing
from SparkEnvironment import SparkEnvironment
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


# Environment
load_dotenv()
sk = SparkEnvironment(session_type='local')
ex = Extraction(s3env = dc.s3, spark_environment=sk.spark_environment)
pp = PreProcessing(spark_environment=sk.spark_environment)

# Oppen Dataset
dataset = pd.read_csv(os.path.join(ex.PATH_PRE_PROCESSED, 'pp_revenue_quarter.csv'))

X = dataset.drop(columns=['CNPJ_CIA', 'RESULTADO_BRUTO']).values
y = dataset['RESULTADO_BRUTO'].values

# train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=7)

# Training
params = {'min_samples_leaf':range(1, 110, 10),
          'max_depth':range(5, 65, 6)}

# RandomizedSearchCV
model = RandomForestRegressor()
rdModel = RandomizedSearchCV(estimator=model, param_distributions=params, n_iter=2, scoring='neg_root_mean_squared_error', cv=5, verbose=1)
rdModel.fit(X, y)

# Fitting Main Model
print(rdModel.best_estimator_)
model = RandomForestRegressor(max_depth=29, min_samples_leaf=41)
model.fit(X_train, y_train)
# Predictions
y_pred_train = model.predict(X_train)
y_pred_test = model.predict(X_test)

# Metrics
mae = mean_absolute_error(y_test, y_pred_test)
print('MAE: %.2f' % mae)
mse = mean_squared_error(y_test, y_pred_test)
print('MSE: %.2f' % mse)
# Overfitting
r2 = r2_score(y_train, y_pred_train)
print('R2: %.2f' % r2)
r2 = r2_score(y_test, y_pred_test)
print('R2: %.2f' % r2)

# Saving Model
DIR_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath('__file__'))), 'models_files')
joblib.dump(model, os.path.join(DIR_PATH, 'modelRevenue.joblib'))