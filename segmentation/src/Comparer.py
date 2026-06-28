import glob
import json
import os
import matplotlib.pyplot as plt

class Comparer:
    def __init__(self, root_dir: str, experiments_folders: list[str]):
        self.root_dir = root_dir
        self.experiments_folders = experiments_folders
        self.dict_info = {folder: {} for folder in experiments_folders}
        self._get_path_to_jsons()
        
        for exp in self.dict_info.values():
            exp['stats'] = self._get_stats_for_one(exp['stat_json'])

    def create_graphs(self, list_stats: list[str], save_dir = ''):
        for stat in list_stats:
            if save_dir != '':
                os.makedirs(save_dir, exist_ok=True)
                self.create_graph(stat, os.path.join(save_dir, f'{stat}.png'))
            else:
                self.create_graph(stat)

    def _get_path_to_jsons(self):
        # найдём все json файлы
        json_files = glob.glob(f"{self.root_dir}/**/*.json", recursive=True)
        # Проходим по папкам с экспериментами и выбираем директории только папки, т.к. в них хранятся эксперименты
        for folder in self.experiments_folders:
            list_dir = os.listdir(os.path.join(self.root_dir, folder))
            exp_folder = [f for f in list_dir if os.path.isdir(os.path.join(self.root_dir, folder, f))][0]
            exp_json = [j for j in  json_files if os.path.splitext(os.path.split(j)[1])[0] == exp_folder][0]
            self.dict_info[folder]['stat_json'] = exp_json

    def _get_stats_for_one(self, json_path: str):
        json_info = []
        with open(json_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            for line in content.split('\n'):
                if line:
                    json_info.append(json.loads(line))

        even_keys = json_info[0].keys()
        odd_keys = json_info[1].keys()
        res = {key: [] for key in [*even_keys, *odd_keys]}

        for i in range(0, len(json_info), 2):
            for key in even_keys:
                res[key].append(json_info[i][key])
            for key in odd_keys:
                res[key].append(json_info[i + 1][key])

        return res

    def create_graph(self, stat_key: str = 'loss', save_path = ''):
        plt.figure(figsize=(10, 5))
        plt.title(f'Динамика изменения {stat_key}')
        for exp_name in self.dict_info: 
            exp = self.dict_info[exp_name]['stats']
            values = exp[stat_key]
            plt.plot(list(range(len(values))), values, label=exp_name.replace('1em', '1e-')) #переименовываем имя, чтобы было понятно, что это lr
        plt.xlabel('epoch')
        plt.ylabel(stat_key)
        plt.legend()
        plt.grid(True)

        if save_path != '':
            plt.savefig(save_path)
        plt.show()

    def create_graph_few_metric(self, exp_name:str, stat_keys: list[str], stats_name: str = '',save_path: str = ''):
        plt.figure(figsize=(10, 5))
        plt.title(f'Динамика изменения {stats_name} для эксперимента {exp_name}')
        for stat_key in stat_keys: 
            exp = self.dict_info[exp_name]['stats']
            values = exp[stat_key]
            plt.plot(list(range(len(values))), values, label=stat_key) #переименовываем имя, чтобы было понятно, что это lr
        plt.xlabel('epoch')
        plt.legend()
        plt.grid(True)

        if save_path != '':
            save_dir = os.path.split(save_path)[0]
            os.makedirs(save_dir, exist_ok=True)
            plt.savefig(save_path)
        plt.show()    
            