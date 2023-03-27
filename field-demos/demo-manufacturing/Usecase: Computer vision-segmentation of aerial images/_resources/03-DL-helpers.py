# Databricks notebook source
# MAGIC %md #helpers for DS / DL 
# MAGIC 
# MAGIC This notebook contains the functions required to train our model.

# COMMAND ----------

# DBTITLE 1,Image transformation for pytorch
from functools import partial
import torchvision

# Transform our images to be ready for the ML model
def transform_row(is_train, batch_pd):
  
  # transform images
  # -----------------------------------------------------------
  # transform step 1: read incoming content value as an image
  transformersImage = [torchvision.transforms.Lambda(lambda x: (np.moveaxis(np.array(Image.open(io.BytesIO(x))), -1, 0)).copy())]
  transformersMask = [torchvision.transforms.Lambda(lambda x:  np.minimum(np.expand_dims(np.array(Image.open(io.BytesIO(x))), 0),1))]

  # assemble transformation steps into a pipeline
  transImg = torchvision.transforms.Compose(transformersImage)
  transMask = torchvision.transforms.Compose(transformersMask)
  
  # apply pipeline to images 
  batch_pd['image'] = batch_pd['content'].map(lambda x: transImg(x))
  
  # -----------------------------------------------------------
  # transform labels (our evaluation metric expects values to be float32)
  # -----------------------------------------------------------
  batch_pd['mask'] = batch_pd['mask'].map(lambda x: transMask(x))
  # -----------------------------------------------------------
  return batch_pd[['image', 'mask']]
 
# define function to retrieve transformation spec
def get_transform_spec(is_train=True):
  return TransformSpec(
            partial(transform_row, is_train), # function to call to retrieve/transform row
            edit_fields=[  # schema of rows returned by function
                ('image', np.uint8, (3, IMAGE_RESIZE, IMAGE_RESIZE), False), 
                ('mask', np.uint8, (1, IMAGE_RESIZE, IMAGE_RESIZE), False)], 
            selected_fields=['image', 'mask'])

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Actual Boat detection model using segmentation_models_pytorch
# MAGIC 
# MAGIC Actual model using segmentation_models_pytorch

# COMMAND ----------

import pytorch_lightning as pl
import segmentation_models_pytorch as smp
from torch.utils.data import Dataset, DataLoader

class BoatModel(pl.LightningModule):

    def __init__(self, arch, encoder_name, in_channels, out_classes, lr, **kwargs):
        super().__init__()
        self.model = smp.create_model(arch, encoder_name=encoder_name, in_channels=in_channels, classes=out_classes, **kwargs)

        # preprocessing parameteres for image
        params = smp.encoders.get_preprocessing_params(encoder_name)
        self.register_buffer("std", torch.tensor(params["std"]).view(1, 3, 1, 1))
        self.register_buffer("mean", torch.tensor(params["mean"]).view(1, 3, 1, 1))

        # for image segmentation dice loss could be the best first choice
        self.loss_fn = smp.losses.DiceLoss(smp.losses.BINARY_MODE, from_logits=True)
        self.lr=lr

    def forward(self, image):
        # normalize image here
        image = (image - self.mean) / self.std
        mask = self.model(image)
        return mask

    def shared_step(self, batch, stage):       
        image = batch["image"]
        # Shape of the image should be (batch_size, num_channels, height, width)
        # if you work with grayscale images, expand channels dim to have [batch_size, 1, height, width]
        assert image.ndim == 4

        # Check that image dimensions are divisible by 32, 
        # encoder and decoder connected by `skip connections` and usually encoder have 5 stages of 
        # downsampling by factor 2 (2 ^ 5 = 32); e.g. if we have image with shape 65x65 we will have 
        # following shapes of features in encoder and decoder: 84, 42, 21, 10, 5 -> 5, 10, 20, 40, 80
        # and we will get an error trying to concat these features
        h, w = image.shape[2:]
        assert h % 32 == 0 and w % 32 == 0

        mask = batch["mask"]
        # Shape of the mask should be [batch_size, num_classes, height, width]
        # for binary segmentation num_classes = 1
        assert mask.ndim == 4

        # Check that mask values in between 0 and 1, NOT 0 and 255 for binary segmentation
        assert mask.max() <= 1.0 and mask.min() >= 0

        logits_mask = self.forward(image)
        # Predicted mask contains logits, and loss_fn param `from_logits` is set to True
        loss = self.loss_fn(logits_mask, mask)

        # Lets compute metrics for some threshold
        # first convert mask values to probabilities, then 
        # apply thresholding
        prob_mask = logits_mask.sigmoid()
        pred_mask = (prob_mask > 0.5).float()

        # We will compute IoU metric by two ways
        #   1. dataset-wise
        #   2. image-wise
        # but for now we just compute true positive, false positive, false negative and
        # true negative 'pixels' for each image and class
        # these values will be aggregated in the end of an epoch
        tp, fp, fn, tn = smp.metrics.get_stats(pred_mask.long(), mask.long(), mode="binary")
        return {
            "loss": loss,
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
        }

    def shared_epoch_end(self, outputs, stage):
        # aggregate step metics
        tp = torch.cat([x["tp"] for x in outputs])
        fp = torch.cat([x["fp"] for x in outputs])
        fn = torch.cat([x["fn"] for x in outputs])
        tn = torch.cat([x["tn"] for x in outputs])

        # per image IoU means that we first calculate IoU score for each image 
        # and then compute mean over these scores
        per_image_iou = smp.metrics.iou_score(tp, fp, fn, tn, reduction="micro-imagewise")
        
        # dataset IoU means that we aggregate intersection and union over whole dataset
        # and then compute IoU score. The difference between dataset_iou and per_image_iou scores
        # in this particular case will not be much, however for dataset 
        # with "empty" images (images without target class) a large gap could be observed. 
        # Empty images influence a lot on per_image_iou and much less on dataset_iou.
        dataset_iou = smp.metrics.iou_score(tp, fp, fn, tn, reduction="micro")

        metrics = {
            f"{stage}_per_image_iou": per_image_iou,
            f"{stage}_dataset_iou": dataset_iou,
        }
        
        self.log_dict(metrics, prog_bar=True)
        mlflow.log_metric(f"{stage}_per_image_iou",per_image_iou.item())
        mlflow.log_metric(f"{stage}_dataset_iou",dataset_iou.item())

    def training_step(self, batch, batch_idx):
        return self.shared_step(batch, "train")            

    def training_epoch_end(self, outputs):
        return self.shared_epoch_end(outputs, "train")

    def validation_step(self, batch, batch_idx):
        return self.shared_step(batch, "valid")

    def validation_epoch_end(self, outputs):
        return self.shared_epoch_end(outputs, "valid")

    def test_step(self, batch, batch_idx):
        return self.shared_step(batch, "test")  

    def test_epoch_end(self, outputs):
        return self.shared_epoch_end(outputs, "test")

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.lr)


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Custom model wrapper for MLFlow
# MAGIC 
# MAGIC While we could save our model using an MLFlow pytorch flavor, we add a custom wrapper around it to make inference easier.
# MAGIC 
# MAGIC The wrapper is flexible: it allows input as base64 or binary, will resize the image to the expected size and run the model inference.
# MAGIC 
# MAGIC Base64 will be useful for our interactive real-time demo inference

# COMMAND ----------

import pandas as pd
import numpy as np
import torch
import base64
from PIL import Image
import io

class CVModelWrapper(mlflow.pyfunc.PythonModel):
  
  def __init__(self, model):     
    # instantiate model in evaluation mode
    self.model = model.eval()
    self.output_base64 = False

  def prep_data(self, data):
    if isinstance(data, str):
      self.output_base64 = True
      data = base64.b64decode(data)
    return torch.tensor(np.moveaxis(np.array(Image.open(io.BytesIO(data)).convert("RGB").resize((256, 256), Image.LINEAR)), -1, 0))
    
  def predict(self, context, model_input):
    #if we have a dataframe, take the first column as input (regardless of the name)
    if isinstance(model_input, pd.DataFrame):
      model_input = model_input.iloc[:, 0]
    # transform input images
    features = model_input.map(lambda x: self.prep_data(x))
    
    # make predictions
    outputs = []
    for i in torch.utils.data.DataLoader(features):
      with torch.no_grad():
        o = self.model(i)
      pr_masks = o.sigmoid()
      
      for pr_mask in pr_masks:        
        img = (pr_mask.detach().numpy().squeeze()*255).astype(np.uint8)
        image=Image.fromarray(img, mode='L')
        output = io.BytesIO()
        image.save(output, format='JPEG')
        output_value = output.getvalue()
        if self.output_base64:
          output_value = base64.b64encode(output_value).decode()
        outputs += [output_value]
    return pd.Series(outputs)
