import torch
from mmengine.config import Config
from mmdet.apis import init_detector, inference_detector
from mmdet.utils import register_all_modules
from mmengine.registry import DATASETS
from torch.utils.data import DataLoader
from mmengine.dataset import default_collate
from tqdm import tqdm
from pycocotools.coco import COCO
from pycocotools.cocoeval import COCOeval
import numpy as np
import os
from ultralytics import YOLO
import time

class Comparer:
    def __init__(self, yolo_weights_path: str, fcos_config_path: str, fcos_weights_path: str, images_dir: str):
        self.yolo_model = YOLO(yolo_weights_path).eval()

        register_all_modules()
        self.fcos_model = init_detector(fcos_config_path, fcos_weights_path, device='cuda').eval()
        self.fcos_config_path = fcos_config_path

        test_dit = "datasets/minecraft/test/"
        image_ext = ('.jpg', '.jpeg', '.png')

        list_files = os.listdir(images_dir) 
        all_images = [f for f in list_files
                    if os.path.isfile(os.path.join(test_dit, f)) 
                    and f.lower().endswith(image_ext)]
        self.images_dir = images_dir
        self.all_images = all_images

    def compar_models(self):
        print('Начато сравнение моделей')
        yolo_metrics = self._get_yolo_metrics()
        fcos_metrics = self._get_fcos_metrics()        
        print('Начато измерение fps')
        yolo_metrics['fps'] = self._measure_fps(self.yolo_model, True)
        fcos_metrics['fps'] = self._measure_fps(self.fcos_model, False)
        return {
            'yolo_metrics': yolo_metrics,
            'fcos_metrics': fcos_metrics
        }

    def _get_yolo_metrics(self, data: str ='datasets/minecraft/yolo/data.yaml', split: str = 'test'):
        metrics = self.yolo_model.val(
            data=data,
            split=split, 
            verbose=False
        )
        return {
            'map_50': round(metrics.box.map50, 4),
            'map_50_95': round(metrics.box.map, 4)
        }

    def _get_fcos_metrics(self):
        register_all_modules()
        cfg = Config.fromfile(self.fcos_config_path)

        # Тестовый датасет из конфига
        test_dataloader_cfg = cfg.test_dataloader
        dataset_cfg = test_dataloader_cfg.dataset
        dataset_cfg.test_mode = True
        dataset = DATASETS.build(dataset_cfg)

        # аннотации и правильные разметки
        ann_file = cfg.test_evaluator.ann_file  
        coco_gt = COCO(ann_file)

        dataloader = DataLoader(
            dataset,
            batch_size=1,
            shuffle=False,
            num_workers=test_dataloader_cfg.get('num_workers', 2),
            collate_fn=default_collate
        )

        # Инициализируем модель
        device = 'cuda:0' if torch.cuda.is_available() else 'cpu'

        cat_ids = dataset.cat_ids 

        # Прогноз результатов
        coco_results = []
        with torch.no_grad():
            for data_batch in tqdm(dataloader, desc='Testing'):
                data_sample = data_batch['data_samples'][0]
                img_id = data_sample.img_id
                
                # Предсказания
                result = self.fcos_model.test_step(data_batch)[0]
                pred_instances = result.pred_instances
                if len(pred_instances) == 0:
                    continue
                
                bboxes = pred_instances.bboxes.cpu().numpy()
                scores = pred_instances.scores.cpu().numpy()
                labels = pred_instances.labels.cpu().numpy()
                
                for bbox, score, label in zip(bboxes, scores, labels):
                    x1, y1, x2, y2 = bbox
                    w = x2 - x1
                    h = y2 - y1
                    category_id = cat_ids[label]
                    coco_results.append({
                        'image_id': int(img_id),
                        'category_id': int(category_id),
                        'bbox': [float(x1), float(y1), float(w), float(h)],
                        'score': float(score)
                    })

        # метрики COCO
        coco_dt = coco_gt.loadRes(coco_results)
        coco_eval = COCOeval(coco_gt, coco_dt, iouType='bbox')
        coco_eval.evaluate()
        coco_eval.accumulate()
        coco_eval.summarize()
        map_50_95 = coco_eval.stats[0]
        map_50 = coco_eval.stats[1]
        return {
            'map_50': round(map_50, 4),
            'map_50_95': round(map_50_95, 4)
        }

    def _measure_fps(self, model, is_yolo: bool):
        img_path = os.path.join(self.images_dir, self.all_images[0])

        #прогрев
        if is_yolo:
            result = model.predict(img_path, verbose=False)
        else:
            result = inference_detector(model, img_path)

        #полный прогон изображений            
        start_time = time.time()
        for image in self.all_images:
            img_path = os.path.join(self.images_dir, image)
            if is_yolo:
                result = model.predict(img_path,  verbose=False)
            else:
                result = inference_detector(model, img_path)
        end_time = time.time()
        fps = len(self.all_images) / (end_time - start_time)
        return fps
        