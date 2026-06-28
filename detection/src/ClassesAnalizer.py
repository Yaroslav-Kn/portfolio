import json
import os
import matplotlib.pyplot as plt
from collections import Counter

class ClassesAnalizer:
    def __init__(self, dataset_path: str, name_annot: str = 'annotations.json', dataset_name = '', dir_save_images='artifacts/datasets_info'):
        self.dataset_path = dataset_path
        self.annotaion_path = os.path.normpath(os.path.join(dataset_path, name_annot))
        self.dataset_name = dataset_name
        self.dir_save_images = dir_save_images
        if not os.path.exists(self.annotaion_path):
            raise FileNotFoundError(f"Файл с аннотациями {self.annotaion_path} не найден")

        with open(self.annotaion_path, 'r', encoding='utf-8') as f:
            self.data = json.load(f)

    def show_obj_per_image(self):
        count_obj_per_image = list(Counter([a['image_id'] for a in self.data['annotations']]).values())

        plt.figure(figsize= (6, 4))
        plt.hist(count_obj_per_image)
        title = 'Количество мобов на изображении'
        if self.dataset_name != '':
            title += f' для датасета {self.dataset_name}'
        plt.title(title)
        plt.xlabel('Количество мобов')
        plt.ylabel('Количество изображений')
        save_path = os.path.join(self.dir_save_images, f'{self.dataset_name}_mobs_per_image.png')
        plt.savefig(save_path)
        plt.show()

    def show_classes_per_image(self):
        imag_dict = {}
        for annot in self.data['annotations']:
            if annot['image_id'] not in imag_dict:
                imag_dict[annot['image_id']] = set()
            imag_dict[annot['image_id']].add(annot['category_id'])
        count_classes_per_image = [len(img) for img in imag_dict.values()]

        plt.figure(figsize= (6, 4))
        plt.hist(count_classes_per_image)
        title = 'Количество классов на изображении'
        if self.dataset_name != '':
            title += f' для датасета {self.dataset_name}'
        plt.title(title)
        plt.xlabel('Количество классов')
        plt.ylabel('Количество изображений')
        save_path = os.path.join(self.dir_save_images, f'{self.dataset_name}_class_per_image.png')
        plt.savefig(save_path)
        plt.show()

    def show_count_classes(self):
        dict_cat = {cat['id']: cat['name'] for cat in self.data['categories']}
        count_classes = Counter([a['category_id'] for a in self.data['annotations']])
        count_classes = sorted(count_classes.items(), key=lambda x: x[1], reverse=False)
        x = [pair[1] for pair in count_classes]
        y = [dict_cat[pair[0]] for pair in count_classes]

        plt.figure(figsize= (6, 4))
        plt.barh(y, x)
        title = 'Распределение классов'
        if self.dataset_name != '':
            title += f' для датасета {self.dataset_name}'
        plt.title(title)
        plt.xlabel('Количество вхождений класса')
        plt.ylabel('Название класса')
        save_path = os.path.join(self.dir_save_images, f'{self.dataset_name}_count_classes.png')
        plt.savefig(save_path)
        plt.show()
        