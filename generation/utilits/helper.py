import os
import torch
from peft import get_peft_model_state_dict

import matplotlib.pyplot as plt
import numpy as np

def save_checkpoint(pipe, optimizer, step, save_dir="checkpoints"):
    os.makedirs(save_dir, exist_ok=True)
    checkpoint_path = os.path.join(save_dir, f"checkpoint_step_{step}")
    
    lora_state_dict = get_peft_model_state_dict(pipe.unet)
    pipe.save_lora_weights(
        save_directory=checkpoint_path,
        unet_lora_layers=lora_state_dict,
        safe_serialization=True
    )
    
    torch.save({
        'optimizer_state_dict': optimizer.state_dict(),
        'step': step
    }, os.path.join(checkpoint_path, f"optimizer_state_{step}.pt"))    
    print(f"Чекпоинт сохранен: {checkpoint_path}")

def draw_loss_graph(losses, path_to_save):
    plt.figure(figsize=(10, 6))
    plt.plot(losses, alpha=0.7, label="Loss")
    plt.plot(range(0, len(losses), 50), 
            [np.mean(losses[i:i+50]) for i in range(0, len(losses), 50)], 
            'r-', linewidth=2, label="Среднее за 50 шагов")
    plt.xlabel("Шаг обучения")
    plt.ylabel("Loss")
    plt.title("Динамика обучения LoRA")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig(path_to_save)
    plt.show()