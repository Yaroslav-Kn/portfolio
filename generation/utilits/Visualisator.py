import os

import cv2
from PIL import Image
import matplotlib.pyplot as plt

class Visualisator:
    def __init__(self, images_paths: str, save_path: str):
        self.images_path = images_paths
        self.save_path = save_path
        files = os.listdir(images_paths)
        self.image_paths = [
            os.path.join(images_paths, img) 
            for img in files 
            if os.path.splitext(img)[1] in ['.png', '.jpg']
            ]

    def visualize(self, title: str, save_img_name: str):
        num_lines = len(title.split('\n'))
        top_margin = 1 - (num_lines * 0.05) 

        fig, axes = plt.subplots(1, 3, figsize=(15, 5)) # Немного увеличили высоту
        
        plt.suptitle(title, y=0.92, fontsize=16, va='top', linespacing=1.5)
        
        for i, path in enumerate(self.image_paths[:3]):
            img = cv2.imread(path)
            img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
            axes[i].imshow(img)
            axes[i].axis('off')
        
        plt.tight_layout(rect=[0, 0, 1, top_margin])
        
        plt.savefig(os.path.join(self.save_path, save_img_name), bbox_inches='tight')
        plt.show()

