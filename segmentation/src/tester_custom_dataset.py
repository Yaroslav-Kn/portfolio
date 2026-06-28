import os 
from PIL import Image
import tempfile

from mmseg.datasets import CustomDataset
from mmseg.structures import SegDataSample
from mmengine.structures import PixelData
from mmseg.visualization import SegLocalVisualizer
from mmengine.registry import init_default_scope

init_default_scope('mmseg')

def load_practice_ds() -> CustomDataset:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_root = os.path.join(project_root, "datasets")
    data_prefix=dict(img_path=os.path.join("img", "test"), seg_map_path=os.path.join("labels", "test"))
    # Пайплайн данных у нас не меняется, загружаем картинки и разметку 
    train_pipeline = [
        dict(type='LoadImageFromFile'),
        dict(type='LoadAnnotations'),
    ]

    dataset = CustomDataset(
        data_root=data_root, 
        data_prefix=data_prefix, 
        test_mode=False, 
        pipeline=train_pipeline,
        img_suffix=".jpg",
        seg_map_suffix=".png"
    )
    return dataset 

def plot_sample_demo(ds, idx=0):
    print(f"Загружен датасет длиной {len(ds)} элементов")
    
    # Получаем метаинформацию
    ds_meta = ds.metainfo
    
    # Создаем временную директорию для сохранения
    with tempfile.TemporaryDirectory() as tmpdir:
        seg_local_visualizer = SegLocalVisualizer(
            vis_backends=[dict(type='LocalVisBackend', save_dir=tmpdir)],
        )
        
        seg_local_visualizer.dataset_meta = dict(
            classes=ds_meta["classes"],
            palette=ds_meta["palette"]
        )
        
        sample = ds[idx]
        plot_sample = SegDataSample()
        plot_sample.gt_sem_seg = PixelData(data=sample["gt_seg_map"])
        
        seg_local_visualizer.add_datasample(
            name="test_sample",
            image=sample["img"],
            data_sample=plot_sample,
            show=False,
            draw_pred=False,
            out_file=os.path.join(tmpdir, "vis.png") 
        )
        
        img_path = os.path.join(tmpdir, "vis.png")
        if os.path.exists(img_path):
            display(Image.open(img_path))
        else:
            print("Не удалось создать визуализацию")
