import os
from mmengine.config import Config
from mmengine.runner import Runner
from mmseg.apis import init_model, inference_model
from mmseg.utils import register_all_modules
from mmseg.visualization import SegLocalVisualizer
import mmcv
import cv2
from tqdm import tqdm

def predict_mask_for_test(config_path: str, checkpoint_path: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    register_all_modules()
    cfg = Config.fromfile(config_path)
    model = init_model(cfg, checkpoint_path, device='cuda:0')

    test_loader = Runner.build_dataloader(cfg.test_dataloader)
    for data_batch in tqdm(test_loader, desc='Создание прогноза на тестовом датасете'):
        img_paths = [data_sample.img_path for data_sample in data_batch['data_samples']]
        results = inference_model(model, img_paths)
        
        for result, img_path in zip(results, img_paths):
            img_name = os.path.basename(img_path).replace('.jpg', '.png')
            save_path = os.path.join(output_dir, img_name)
            
            mask = result.pred_sem_seg.data[0].cpu().numpy()
            cv2.imwrite(save_path, mask)

def get_metrics_for_test(config_path: str, checkpoint_path: str, work_dir: str):
    register_all_modules()
    cfg = Config.fromfile(config_path)
    cfg.work_dir = work_dir
    cfg.device = 'cuda'
    runner = Runner.from_cfg(cfg)
    runner.load_checkpoint(checkpoint_path)

    metrics = runner.test()
    return metrics

def get_mask_visualisation(config_path: str, checkpoint_path: str, output_dir: str):    
    os.makedirs(output_dir, exist_ok=True)
    register_all_modules()
    cfg = Config.fromfile(config_path)
    model = init_model(cfg, checkpoint_path, device='cuda:0')

    visualizer = SegLocalVisualizer(
        vis_backends=[dict(type='LocalVisBackend')],
        save_dir=output_dir,
        alpha=0.5
    )
    visualizer.dataset_meta = model.dataset_meta
    test_loader = Runner.build_dataloader(cfg.test_dataloader)

    for data_batch in tqdm(test_loader, desc='Создание изображений с масками'):
        img_paths = [ds.img_path for ds in data_batch['data_samples']]
        results = inference_model(model, img_paths)
        
        for result, img_path in zip(results, img_paths):
            image = mmcv.imread(img_path)
            image = mmcv.imconvert(image, 'bgr', 'rgb')
            
            img_name = os.path.basename(img_path)
            visualizer.add_datasample(
                name=img_name,
                image=image,
                data_sample=result,
                draw_gt=False,
                wait_time=0,
                out_file=os.path.join(output_dir, img_name)
            )
            