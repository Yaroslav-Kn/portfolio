import pandas as pd
import joblib
import json
import yaml

from sklearn.metrics import mean_absolute_error, root_mean_squared_error

def evaluate_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    with open('models/fitted_model.pkl', 'rb') as fd:
        pipeline = joblib.load(fd) 

    target_col = params['target_col']
    data_test = pd.read_csv('data/test_data.csv')   
    X_test = data_test.drop(['id', target_col], axis=1)
    y_true = data_test[target_col]

    y_pred = pipeline.predict(X_test)

    res = {
         'mae': round(mean_absolute_error(y_true, y_pred)),
         'rmse': round(root_mean_squared_error(y_true, y_pred))
    }

    with open('cv_results/cv_res.json', 'w') as fp:
        json.dump(res, fp)

if __name__ == '__main__':
	evaluate_model()
