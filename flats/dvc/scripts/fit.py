import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from category_encoders import CatBoostEncoder
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import yaml
import os
import joblib

def fit_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)     

    data = pd.read_csv('data/initial_data.csv')

    train_dara, test_data =  train_test_split(data,
                                              random_state=159,
                                              test_size=params['test_size'])
    
    target_col = params['target_col']
    train_features = train_dara.drop(['id', target_col], axis=1)

    num_features = train_features.select_dtypes(include=['float', 'int'])
    cat_bin_features = train_features.select_dtypes(include=['bool'])
    cat_unbin_features = train_features.select_dtypes(include='object') 

    preprocessor  = ColumnTransformer(
        [
            ('binary', OneHotEncoder(drop=params['one_hot_drop']), cat_bin_features.columns.tolist()),
            ('cat', CatBoostEncoder(return_df=False), cat_unbin_features.columns.tolist()),
            ('num', StandardScaler(), num_features.columns.tolist())
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )

    model = LinearRegression()

    pipeline = Pipeline(
        [
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )   

    pipeline.fit(train_features, train_dara[target_col])

    os.makedirs('models', exist_ok=True)
    with open('models/fitted_model.pkl', 'wb') as fd:
        joblib.dump(pipeline, fd) 

    test_data.to_csv('data/test_data.csv', index=False)

if __name__ == '__main__':
    fit_model()