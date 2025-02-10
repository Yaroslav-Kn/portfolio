#!/bin/bash
source ./.env
export S3_BUCKET_NAME=$S3_BUCKET_NAME
export S3_ENDPOINTURL=$S3_ENDPOINTURL

cd dvc
dvc init --subdir # инициализация (subdir, т.к. запускаем из подкаталога)
dvc remote add -d my_storage s3://$S3_BUCKET_NAME # подключаем S3
dvc remote modify my_storage endpointurl $S3_ENDPOINTURL # указываем экдпоинт для S3
dvc remote modify --local my_storage credentialpath '../.env' # указываем путь для кридов
dvc remote modify my_storage version_aware true # разрешаем версионирование файлов для хранилища