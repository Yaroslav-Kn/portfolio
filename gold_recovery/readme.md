# Gold_recovery - ML

Задача посвящена проблеме рассчёта эффективности показателей добычи золота. Необходимо из имеющихся данных предсказать, какие значения получатся в том или ином технологическом процессе.

## Поставленные цели

Обучить ряд моделей и выбрать лучшую, позволяющую предсказать какое количество золота можно получить при конкретных показателях. Задача осложняется тем, что необходимо использовать не стандартную метрику качества, а sMAPE (симметричное среднее абсолютное процентное отклонение). Помимо этого итоговая метрика для всего переходасостоит из 25% sMAPE на операции флотации (операция в ходе которой получают черновой концентрат) и 75% sMAPE после финальной очистки.

## Достигнутые результаты

Проанализированы исходные данные, изучены зависимости и концентрации металлов на разных операциях обогощения. Написаны функции для рассчёта показателей метрики качества. Обучено несколько моделей и выбрана лучшая, которой оказалась модель CatBost для обоих переходов и позволившая получить результат на тестовых данных равный 9.07.

## Стек используемых технологий

В проекте используется следующий стек технологий Python:

* pandas
* seaborn
* matplotlib

* LinearRegression
* DecisionTreeRegressor
* RandomForestRegressor
* CatBoostRegressor

* GridSearchCV
* cross_val_score