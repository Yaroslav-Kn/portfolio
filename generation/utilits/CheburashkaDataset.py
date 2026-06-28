from PIL import Image
from torchvision import transforms
from torch.utils.data import Dataset

class CheburashkaDataset(Dataset):
    def __init__(self, image_paths, size=512):
        self.image_paths = image_paths
        self.transform = transforms.Compose([
            transforms.Resize((size, size)),
            transforms.RandomHorizontalFlip(p=0.5),
            transforms.RandomRotation(degrees=15),
            transforms.ToTensor(),
            transforms.Normalize([0.5], [0.5])
        ])

    def __len__(self):
        return len(self.image_paths)

    def __getitem__(self, idx):
        img = Image.open(self.image_paths[idx]).convert('RGB')
        return self.transform(img)
        