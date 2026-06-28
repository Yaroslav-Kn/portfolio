import json
import os
import glob
import random
import cv2
import matplotlib.pyplot as plt


class DataVerifier:
    def __init__(self, dataset_path: str, name_annot: str = 'annotations.json', random_state: int =159):
        self.dataset_path = dataset_path
        self.annotaion_path = os.path.normpath(os.path.join(dataset_path, name_annot))
        if not os.path.exists(self.annotaion_path):
            raise FileNotFoundError(f"Файл с аннотациями {self.annotaion_path} не найден")

        with open(self.annotaion_path, 'r', encoding='utf-8') as f:
            self.data = json.load(f)
        random.seed(random_state)

    def verify_json(self):
        self._check_key(self.data, 'info')
        self._check_key(self.data, 'categories')
        self._check_key(self.data, 'images')
        self._check_key(self.data, 'annotations')
        print('-' * 50)
        print(f'В аннотации указано {len(self.data["categories"])} категорий')
        print(f'В аннотации указано {len(self.data["images"])} изображений')
        print(f'В аннотации размечено {len(self.data["annotations"])} размеченных сущностей')

    def _check_key(self, data: dict, key: str):
        info = data.get(key, None)
        if data.get(key, None) is  None:
            raise KeyError(f'Не обнаружен ключ {key}')    

    def verify_images(self):
        list_images_files = self._get_list_images()
        list_images_annot = [os.path.normpath(os.path.join(self.dataset_path, i['file_name'])) for i in self.data['images']]
        print('-' * 50)
        print(f'Обнаружено {len(list_images_files)} файлов изображений')
        print(f'Обнаружено {len(list_images_annot)} ссылок на изображения в аннотациях')

        images_without_labels, labels_without_images = self._find_missing_pairs(list_images_files, list_images_annot)
        print('-' * 50)
        print(f'Обнаружено {len(images_without_labels)} изображений без аннотаций')
        print(f'Обнаружено {len(labels_without_images)} ссылок на изображения в аннотациях без самих изображений')

    def _get_list_images(self):
        list_images = []
        patterns = ['*.jpg', '*.jpeg', '*.png']
        directory = os.path.normpath(self.dataset_path)
        for end in patterns:
            search_pattern = os.path.join(directory, end)
            found = glob.glob(search_pattern)
            list_images.extend(found)

        return list_images

    def _find_missing_pairs(self, list_images_files, list_images_annot):
        images = set(list_images_files)
        labels = set(list_images_annot)
        images_without_labels = []
        labels_without_images = []

        for image in images:
            if image not in labels:
                images_without_labels.append(image)
        for label in labels:
            if label not in images:
                labels_without_images.append(label)

        return images_without_labels, labels_without_images

    def visualize(self, count_images: int = 3):
        for _ in range(count_images):
            indx = random.randint(0, len(self.data['images']) - 1)
            image_path, image_id, image_annot = self._get_info_for_1img(indx)
            self._visualize_one_image(image_path, image_id, image_annot)

    def _visualize_one_image(self, image_path: str, image_id: int, image_annot: list):
        image = cv2.imread(image_path)
        img_height, img_width, _ = image.shape
        for obj in image_annot:
            bbox = list(map(int, obj['bbox']))
            obj_cat = list(filter(lambda cat: cat['id'] == obj['category_id'], self.data['categories']))[0]
            class_name = obj_cat['name']
            cv2.rectangle(image, (bbox[0], bbox[1]), (bbox[0] + bbox[2], bbox[1] + bbox[3]), (0, 0, 150), 2)
            cv2.putText(image, class_name, (bbox[0], bbox[1] - 10), cv2.FONT_HERSHEY_SIMPLEX, max(0.7, img_width / 1000), (0, 0, 150), 2)

        plt.figure(figsize=(10, 10))
        plt.imshow(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
        plt.title(f"Верификация разметки")
        plt.axis('off')
        plt.show()

    def _get_info_for_1img(self, indx: int) -> tuple:
        file_name = self.data['images'][indx]['file_name']
        image_path = os.path.normpath(os.path.join(self.dataset_path, file_name))
        image_id = self.data['images'][indx]['id']
        image_annot = [annot for annot in self.data['annotations'] if annot['image_id'] == image_id]
        return image_path, image_id, image_annot        
        