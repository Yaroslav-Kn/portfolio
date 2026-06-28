from mmdet.apis import init_detector, inference_detector

from ultralytics import YOLO

import torch

import cv2
import matplotlib.pyplot as plt
import numpy as np

import os
from tqdm import tqdm

class Inferencer:
    def __init__(self, model, is_yolo = False):
        self.model = model
        self.model.eval()
        self.is_yolo = is_yolo
        if is_yolo:
            self.class_names = model.names
        else:
            self.class_names = model.cfg.metainfo.classes
        self.colors = plt.cm.rainbow(np.linspace(0, 1, len(self.class_names)))
        self.colors = (self.colors[:, :3] * 255).astype(np.int32)

    def get_predict_image(self, 
                          img_path: str, 
                          out_path: str, 
                          threshold:float = 0.3, 
                          show: bool = True):
        img = cv2.imread(img_path)

        if self.is_yolo:
            result = self.model.predict(img_path, conf=threshold, verbose=False)
            img = self._create_frame_yolo(img, result, threshold)
        else:
            result = inference_detector(self.model, img_path)
            img = self._create_frame_mmdet(img, result, threshold)
                      
        out_dir = os.path.dirname(out_path)
        os.makedirs(out_dir, exist_ok=True)
        cv2.imwrite(out_path, img)

        if show:
            plt.figure(figsize=(15, 10))
            plt.imshow(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))
            plt.axis('off')
            plt.title('Результаты детекции')
            plt.show()

    def _create_frame_mmdet(self, img, result, threshold):
        pred_instances = result.pred_instances

        bboxes = pred_instances.bboxes.cpu().numpy()
        scores = pred_instances.scores.cpu().numpy()
        labels = pred_instances.labels.cpu().numpy()

        for i in range(len(bboxes)):
            bbox = bboxes[i]
            score = scores[i]
            label = int(labels[i])
            
            if score < threshold:
                continue
            
            color = tuple(map(int, self.colors[label % len(self.colors)]))     
            x1, y1, x2, y2 = bbox.astype(int) 
            cv2.rectangle(img, (x1, y1), (x2, y2), color, 2)   
            label_text = f'{self.class_names[label]}: {score:.2f}'
            cv2.putText(img, label_text, (x1, y1 - 5), cv2.FONT_HERSHEY_SIMPLEX, 
                        0.5, color, 2)  
        return img

    def _create_frame_yolo(self, img, results, threshold):
        for r in results:
            for box in r.boxes:
                x1, y1, x2, y2 = map(int, box.xyxy[0])
                conf, class_id = float(box.conf[0]), int(box.cls[0])

                if conf < threshold:
                    continue
            
                color = tuple(map(int, self.colors[class_id % len(self.colors)]))    
                cv2.rectangle(img, (x1, y1), (x2, y2), color, 2)   
                label_text = f'{self.class_names[class_id]}: {conf:.2f}'
                cv2.putText(img, label_text, (x1, y1 - 5), cv2.FONT_HERSHEY_SIMPLEX, 
                            0.5, color, 2)  
        return img

    def get_predict_video(self, input_video_path, out_path, threshold=0.5):
        cap = cv2.VideoCapture(input_video_path)
        if not cap.isOpened():
            print(f"Ошибка: не удалось открыть видеофайл {input_video_path}")
            return

        # Получаем метаданные видео для корректной записи выходного файла.
        frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = int(cap.get(cv2.CAP_PROP_FPS))
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        # Инициализируем объект для записи видео с кодеком 'mp4v'.
        out_dir = os.path.dirname(out_path)
        os.makedirs(out_dir, exist_ok=True)
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(out_path, fourcc, fps, (frame_width, frame_height))
    
        with tqdm(total=total_frames, desc="Анализ игрового процесса") as pbar:
            while cap.isOpened():
                success, frame = cap.read()
                if not success:
                    break # Видео закончилось



                if self.is_yolo:
                    result = self.model.predict(frame, conf=threshold, verbose=False)
                    frame = self._create_frame_yolo(frame, result, threshold)
                else:
                    result = inference_detector(self.model, frame)
                    frame = self._create_frame_mmdet(frame, result, threshold)

                # # получаем прогноз
                # result = inference_detector(self.model, frame)
                # pred_instances = result.pred_instances

                # bboxes = pred_instances.bboxes.cpu().numpy()
                # scores = pred_instances.scores.cpu().numpy()
                # labels = pred_instances.labels.cpu().numpy()

                # #проходим по каждой рамке
                # for i in range(len(bboxes)):
                #     bbox = bboxes[i]
                #     score = scores[i]
                #     label = int(labels[i])
                    
                #     if score < threshold:
                #         continue
                    
                #     color = tuple(map(int, self.colors[label % len(self.colors)]))     
                #     x1, y1, x2, y2 = bbox.astype(int) 
                #     cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)   
                #     label_text = f'{self.class_names[label]}: {score:.2f}'
                #     cv2.putText(frame, label_text, (x1, y1 - 5), cv2.FONT_HERSHEY_SIMPLEX, 
                #                 0.5, color, 2)   

                # Записываем обработанный кадр в выходной видеофайл.
                out.write(frame)
                pbar.update(1)

        # Освобождаем все ресурсы, чтобы избежать утечек памяти и проблем с файлами.
        cap.release()
        out.release()