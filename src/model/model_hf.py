import re

import torch
import torch.nn as nn
import torchvision.transforms as T
from transformers import AutoFeatureExtractor, AutoModel

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


class ModelHF(nn.Module):
    """Generic wrapper class for loading pre-trained models from HuggingFace hub."""

    def __init__(self, model_ckpt) -> None:
        super().__init__()

        self.model_ckpt = model_ckpt
        self.extractor = AutoFeatureExtractor.from_pretrained(model_ckpt)
        self.model = AutoModel.from_pretrained(model_ckpt)
        self.hidden_dim = self.model.config.hidden_size

    def setupt(self, *args, **kwargs):
        pass

    def extract_embeddings(self, batch):
        """Utility to compute embeddings."""

        images = batch["image"]
        image_batch_transformed = torch.tensor(images).to(device)
        inputs = self.extractor(images=image_batch_transformed, return_tensors="pt")

        with torch.no_grad():
            embeddings = self.model(**inputs).last_hidden_state[:, 0].cpu()
            # Normalize embeddings.
            embeddings /= embeddings.norm(dim=-1, keepdim=True)
        return {"embeddings": embeddings}

    def forward(self, batch):
        return self.extract_embeddings(batch)

    @property
    def name(self):
        # Replace non-alphanumeric characters with underscores.
        return re.sub(r"\W+", "_", self.model_ckpt)

    @property
    def embedding_dim(self):
        return self.hidden_dim
