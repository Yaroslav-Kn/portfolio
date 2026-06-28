import json
import os
import shutil

class Coco2Yolo:
    def __init__(self, base_dir: str, splits: list[str], yolo_dir: str, coco_json: str = 'annotations.json', split_for_name_cat: str = 'train'):
        self.base_dir = base_dir
        self.splits = splits
        self.yolo_dir = yolo_dir
        self.coco_json = coco_json

        with open(os.path.join(base_dir, split_for_name_cat, coco_json), 'r', encoding='utf-8') as f:
            coco_data = json.load(f)
        self.class_map = {}
        for cat in coco_data['categories']:
            self.class_map[cat['id']] = cat['name']

    def convert_all_splits(self):
        for split in self.splits:
            coco_json_path = os.path.join(self.base_dir, split, self.coco_json)
            images_output_dir = os.path.join(self.yolo_dir, split, 'images')
            labels_output_dir = os.path.join(self.yolo_dir, split, 'labels')
            self._convert_coco_to_yolo(coco_json_path, images_output_dir, labels_output_dir)

        yaml_path = os.path.join(self.yolo_dir, 'data.yaml')
        with open(yaml_path, 'w', encoding='utf-8') as f:
            f.write(f"path: ./{self.yolo_dir}\n")
            f.write(f"train: ./train/images\n")
            f.write(f"val: ./valid/images\n")
            if 'test' in self.splits:
                f.write(f"test: ./test/images\n")

            f.write(f"nc: {len(self.class_map)}\n")
            f.write(f"names: {list(self.class_map.values())}\n")

    def _convert_coco_to_yolo(self, coco_json_path, images_output_dir, labels_output_dir):
        with open(coco_json_path, 'r', encoding='utf-8') as f:
            coco_data = json.load(f)

        # Преобразуем информацию об изображениях в словарь с ключом в виде id изображения
        images_info = {}
        for img in coco_data['images']:
            img_id = img['id']
            images_info[img_id] = {
                'file_name': img['file_name'],
                'width': img['width'],
                'height': img['height']
            }

        # аналогично поступаем с аннотациями
        annotations_by_image = {}
        for ann in coco_data['annotations']:
            img_id = ann['image_id']
            annotations_by_image.setdefault(img_id, []).append(ann)

        os.makedirs(images_output_dir, exist_ok=True)
        os.makedirs(labels_output_dir, exist_ok=True)

        #проходим по всем изображениям
        for img_id, img_info in images_info.items():
            input_img_path = os.path.join(os.path.dirname(coco_json_path), img_info['file_name'])
            output_img_path = os.path.join(images_output_dir, img_info['file_name'])

            #если изображение не найдено, то пропускаем
            if os.path.exists(input_img_path):
                shutil.copy2(input_img_path, output_img_path)
            else:
                continue

            # Создаём YOLO-метки
            label_filename = '.'.join(img_info['file_name'].split('.')[:-1] + ['txt'])
            label_path = os.path.join(labels_output_dir, label_filename)

            anns = annotations_by_image.get(img_id, [])
            with open(label_path, 'w', encoding='utf-8') as f:
                for ann in anns:
                    cat_id = ann['category_id']
                    # если класса нет в размеченных индексах, то такой класс пропускаем
                    if cat_id not in self.class_map:
                        continue
                    bbox = ann['bbox']

                    x_center = (bbox[0] + bbox[2] / 2) / img_info['width']
                    y_center = (bbox[1] + bbox[3] / 2) / img_info['height']
                    width = bbox[2] / img_info['width']
                    height = bbox[3] / img_info['height']

                    f.write(f"{cat_id} {x_center:.6f} {y_center:.6f} {width:.6f} {height:.6f}\n")
                    