import pandas as pd
import json
import matplotlib.pyplot as plt

class StudyComparer:
    def __init__(self, fcos_metrics_path: str, yolo_metrics_path: str, path_save_graph: str = 'artifacts/metrics/stady_metrics.png'):
        self.fcos_info = self._get_fcos_info(fcos_metrics_path)
        self.yolo_info = self._get_yolo_info(yolo_metrics_path)
        self.path_save_graph = path_save_graph

    def _get_fcos_info(self, fcos_metrics_path):
        fcos_info = []
        with open(fcos_metrics_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            for line in content.split('\n'):
                if line:
                    fcos_info.append(json.loads(line))

        epoch = []
        mAP_50 = []
        mAP_50_95 = []
        for i in range(0, len(fcos_info), 2):
            epoch.append(fcos_info[i]['epoch'])
            mAP_50.append(fcos_info[i + 1]['coco/bbox_mAP_50'])
            mAP_50_95.append(fcos_info[i + 1]['coco/bbox_mAP'])

        fcos_info = pd.DataFrame(
            {
                'epoch': epoch,
                'fcos_mAP_50': mAP_50,
                'fcos_mAP_50_95': mAP_50_95
            }
        )   
        return fcos_info     

    def _get_yolo_info(self, yolo_metrics_path):
        yolo_info = pd.read_csv(yolo_path)[['epoch', 'metrics/mAP50(B)', 'metrics/mAP50-95(B)']]
        yolo_info = yolo_info.rename(columns={
            'metrics/mAP50(B)': 'yolo_mAP_50',
            'metrics/mAP50-95(B)': 'yolo_mAP_50_95'
        })    
        return yolo_info 

    def create_graph(self):
        plt.figure(figsize=(9, 6))

        plt.subplot(2, 1, 1)
        plt.title('Динамика изменения метрики mAP_50_95 в ходе обучения')
        plt.plot(self.yolo_info['epoch'], self.yolo_info['yolo_mAP_50_95'], label='yolo_mAP_50_95')
        plt.plot(self.fcos_info['epoch'], self.fcos_info['fcos_mAP_50_95'], label='fcos_mAP_50_95')
        plt.legend()
        plt.grid(True)
        
        plt.subplot(2, 1, 2)
        plt.title('Динамика изменения метрики mAP_50 в ходе обучения')
        plt.plot(self.yolo_info['epoch'], self.yolo_info['yolo_mAP_50'], label='yolo_mAP_50')
        plt.plot(self.fcos_info['epoch'], self.fcos_info['fcos_mAP_50'], label='fcos_mAP_50')
        plt.legend()
        plt.grid(True)

        plt.tight_layout()
        plt.savefig(self.path_save_graph)
        plt.show()
        