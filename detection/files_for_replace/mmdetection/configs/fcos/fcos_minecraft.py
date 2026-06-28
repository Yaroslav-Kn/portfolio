_base_ = [
    './fcos_r50-caffe_fpn_gn-head_1x_coco.py',   # базовая архитектура
]

# Информация о класах
metainfo = {
    'classes': ('bee', 'chicken', 'cow', 'creeper', 'enderman', 'fox', 'frog', 'ghast',
                'goat', 'llama', 'pig', 'sheep', 'skeleton', 'spider', 'turtle', 'wolf', 'zombie'),
    'palette': None
}
model = dict(bbox_head=dict(num_classes=len(metainfo['classes'])))

# Размеры изображения после сжатия
img_scale = (512, 512)

# Пайплайны для подготовки данных
train_pipeline = [
    dict(type='LoadImageFromFile'),
    dict(type='LoadAnnotations', with_bbox=True),   
    # цветовые агументации с вероятностью 0.75     
    dict(type='RandomApply',
        transforms=[dict
            (
                type='RandAugment',
                aug_space=[
                    [dict(type='Brightness', level=4)],
                    [dict(type='Contrast', level=3)],
                    [dict(type='Color', level=3)]
                    ],
                aug_num=3 
            )],
        prob=0.75),
    # случайный масштаб, т.к. мобы бывают разных размеров    
    dict(type='RandomChoiceResize',
         scales=[(256, 256),  (384, 384), (512, 512)],
         keep_ratio=True),
    dict(type='RandomFlip', prob=0.5),
    dict(type='PackDetInputs')
]

test_pipeline = [
    dict(type='LoadImageFromFile'),
    dict(type='Resize', scale=img_scale, keep_ratio=True),
    dict(type='PackDetInputs')
]

# Даталоадеры. Трейн с балансировщиком, т.к. есть очень редкие классы
train_dataloader = dict(
    batch_size=32,
    num_workers=2,
    dataset=dict(
        type='ClassBalancedDataset',
        oversample_thr=1e-2,
        dataset=dict(
            type='CocoDataset',
            data_root='datasets/minecraft/',
            ann_file='train/annotations.json',
            data_prefix=dict(img='train/'),
            metainfo=metainfo,
            pipeline=train_pipeline
        )
    )
)

val_dataloader = dict(
    batch_size=32,
    num_workers=2,
    dataset=dict(
        data_root='datasets/minecraft/',
        ann_file='valid/annotations.json',
        data_prefix=dict(img='valid/'),
        metainfo=metainfo,
        test_mode=True,
        pipeline=test_pipeline
    )
)

test_dataloader = dict(
    batch_size=32,
    num_workers=2,
    dataset=dict(
        data_root='datasets/minecraft/',
        ann_file='test/annotations.json',
        data_prefix=dict(img='test/'),
        metainfo=metainfo,
        test_mode=True,
        pipeline=test_pipeline
    )
)

# Пути к аннотациям для оценщика
val_evaluator = dict(
    ann_file='datasets/minecraft/valid/annotations.json'
)
test_evaluator = dict(
    ann_file='datasets/minecraft/test/annotations.json'
)

# Оптимизатор
optim_wrapper = dict(
    _delete_=True,
    type='OptimWrapper',
    optimizer=dict(
        type='AdamW',
        lr=1e-4,
        weight_decay=0.0005
    ))

# scheduler (сначала повышаем lr, затем уменьшим нескольк раз на соответствующих эпохах)
param_scheduler = [
    dict(type='LinearLR', start_factor=0.01, by_epoch=False, begin=0, end=500),
    dict(type='MultiStepLR', begin=0, end=120, by_epoch=True, milestones=[30, 50, 80], gamma=0.5)
]

# Трнируем и делаем валидации каждую эпоху
train_cfg = dict(type='EpochBasedTrainLoop', max_epochs=120, val_interval=1)
# раняя остановка через 20 эпох без улучшения и сохранение лучшей модели по bbox_mAP
default_hooks = dict(
    checkpoint=dict(type='CheckpointHook', 
        interval=5,
        save_best='coco/bbox_mAP',
        rule='greater'
        ),
    early_stopping=dict(
        type='EarlyStoppingHook',
        monitor='coco/bbox_mAP',
        patience=30,   
        rule='greater',
        min_delta=0
    )
)

# Понижаем разрядность
fp16 = dict(loss_scale='dynamic')
