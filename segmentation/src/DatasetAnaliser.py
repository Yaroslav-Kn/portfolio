from collections import Counter
import os
import numpy as np
import random

import matplotlib.pyplot as plt
from PIL import Image

class DatasetAnaliser:
    def __init__(self, root_dir: str, img_dir: str, labels_dir: str, meatinfo: dict, subsets: list[str] = ['train'], seed: int = 159):
        self.root_dir = root_dir
        self.img_dir = img_dir
        self.labels_dir = labels_dir
        self.meatinfo = meatinfo
        self.subsets = subsets
        random.seed(seed)
        np.random.seed(seed)

    def analyze_datasets(self):
        for subset in self.subsets:
            self.verify_dataset(subset)
            self.count_classes_in_subset(subset)
            self.calculate_ratio_area_in_subset(subset)
            self.show_random_img(subset)

    def verify_dataset(self, subset: str = 'train'):
        img_path = os.path.join(self.root_dir, self.img_dir, subset)
        label_path = os.path.join(self.root_dir, self.labels_dir, subset)

        img_files = os.listdir(img_path)
        labels_files = os.listdir(label_path)
        print('-' * 50)
        print(f'Информация о датасете {subset}:')
        print(f'Обнаружено {len(img_files)} изображений')
        print(f'Обнаружено {len(labels_files)} масок')

        img_set = set([os.path.splitext(f)[0] for f in img_files])
        labels_set = set([os.path.splitext(f)[0] for f in labels_files])

        for img in img_set:
            if img not in labels_set:
                print(f'Для изображения {img} отсутсвует маска')

        for label in labels_set:
            if label not in img_set:
                print(f'Для маски {label} отсутсвует изображение')

    def show_random_img(self, subset: str):
        img_files = os.listdir(os.path.join(self.root_dir, self.img_dir, subset))
        img_indx = random.randint(0, len(img_files))
        img_file = img_files[img_indx]        
        img_path = os.path.join(self.root_dir, self.img_dir, subset, img_file)
        img = Image.open(img_path)

        mask_file = os.path.splitext(img_file)[0] + '.png'
        mask_path = os.path.join(self.root_dir, self.labels_dir, subset, mask_file)
        classes_mask =  Image.open(mask_path)

        self.make_segmentation_results_plot(img, classes_mask, subset)

    def make_segmentation_results_plot(self, img: np.ndarray, classes_mask: np.ndarray, subset: str = ''):
        title = 'Пример разметки'
        if subset != '':
            title += f' для датасета {subset}'
        fig, (image_ax, segmentation_map_ax) = plt.subplots(1, 2, figsize=(10, 4))
        fig.suptitle(title)

        image_ax.imshow(img)
        image_ax.set_title("Исходное изображение")
        image_ax.axis('off')

        colored_mask = self.meatinfo['palette'][classes_mask]
        segmentation_map_ax.imshow(colored_mask)
        segmentation_map_ax.set_title("Маска классов")
        segmentation_map_ax.axis('off')

        unique_classes = np.unique(classes_mask)
        patches = [
            segmentation_map_ax.scatter(
                0, 0, color=self.meatinfo['palette'][i] / 255, label=self.meatinfo['classes'][i]
            )
            for i in unique_classes
        ]
        segmentation_map_ax.legend(
            handles=patches, bbox_to_anchor=(1.05, 1), loc="upper left"
        )

        fig.tight_layout()
        plt.show()

    def count_classes_in_subset(self, subset: str):
        label_path = os.path.join(self.root_dir, self.labels_dir, subset)
        labels_files = os.listdir(label_path)
        list_classes = []
        for file in labels_files:
            mask_file = os.path.join(label_path, file)
            list_classes.extend(self.find_classes_in_mask(mask_file))

        count_classes = Counter(list_classes)
        for key, value in count_classes.items():
            print(f'Обнаружено {value} объектов класса {self.meatinfo["classes"][key]}')

    def find_classes_in_mask(self, mask_path: str) -> list[int]:
        classes_mask =  np.array(Image.open(mask_path))
        classes_mask = classes_mask.flatten().tolist()
        return list(set(classes_mask))
    
    def calculate_ratio_area_in_subset(self, subset: str):
        label_path = os.path.join(self.root_dir, self.labels_dir, subset)
        labels_files = os.listdir(label_path)
        list_area = []
        for file in labels_files:
            mask_file = os.path.join(label_path, file)
            list_area.append(self.find_ratio_area_in_mask(mask_file))

        ratio_class_area = {cl: [] for cl in range(len(self.meatinfo["classes"]))}
        for area in list_area:
            for key in area:
                ratio_class_area[key].append(area[key])

        for key, value in ratio_class_area.items():
            print(f'Средняя относительная площадь объектов для класса {self.meatinfo["classes"][key]} = {round(sum(value) / len(value), 4)}')
        self.show_ratio_area(ratio_class_area, subset)

    def find_ratio_area_in_mask(self, mask_path: str) -> dict[float]:
        classes_mask =  np.array(Image.open(mask_path))
        classes_mask = classes_mask.flatten().tolist()
        dict_area = Counter(classes_mask)
        all_area = sum(dict_area.values())

        for key in dict_area:
            dict_area[key] = dict_area[key] / all_area

        return dict_area
        
    def show_ratio_area(self, ratio_class_area: dict[int, list[float]], subset: str = ''):
        title = 'Распределение относительной площади объектов по классам'
        if subset != '':
            title += f' для датасета {subset}'

        all_values = []
        for values in ratio_class_area.values():
            all_values.extend(values)
        bins = 50
        bin_edges = np.linspace(min(all_values), max(all_values), bins + 1)

        plt.figure(figsize=(8, 4))
        plt.title(title)
        for key in ratio_class_area:
            plt.hist(ratio_class_area[key], bins=bin_edges, label=self.meatinfo["classes"][key], alpha=0.5)
        plt.legend()
        plt.show()
        