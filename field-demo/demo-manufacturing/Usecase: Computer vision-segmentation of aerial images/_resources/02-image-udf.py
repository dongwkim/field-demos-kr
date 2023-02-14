# Databricks notebook source
# MAGIC %md 
# MAGIC ## Helpers to handle images

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, col, substring_index, collect_list, size
from PIL import Image
import io

import pandas as pd
IMAGE_RESIZE=256
IMAGE_SIZE=768

# to process a batch of 100 images instead 10000 to reduce memory pressure
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "100")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# COMMAND ----------

# DBTITLE 1,Resize image udf
@pandas_udf("binary")
def resize_image_udf(content_series):
  def resize_image(content):
    """resize image and serialize as jpeg"""
    try:
      image = Image.open(io.BytesIO(content)).resize((IMAGE_RESIZE, IMAGE_RESIZE), Image.NEAREST)
      output = io.BytesIO()
      image.save(output, format='JPEG')
      return output.getvalue()
    except Exception:
      #some images are invalid
      return None
  
  new_images = content_series.apply(resize_image)
  return new_images

resize_image_udf

# COMMAND ----------

# DBTITLE 1,Converting the mask in an image
def rle_decode(mask_rle, shape=(IMAGE_SIZE, IMAGE_SIZE)):
    s = mask_rle.split()
    starts =  np.asarray(s[0::2], dtype=int)
    lengths = np.asarray(s[1::2], dtype=int)
    
    starts -= 1
    ends = starts + lengths
    img = np.zeros(shape[0]*shape[1], dtype=np.uint8)
    for lo, hi in zip(starts, ends):
        img[lo:hi] = 255
    return img.reshape(shape).T 

def mask(rle_masks):
    if isinstance(rle_masks, np.ndarray):
        all_masks = np.zeros((IMAGE_SIZE, IMAGE_SIZE),dtype=np.int8)
        for mask in rle_masks.tolist():
           
            all_masks += rle_decode(mask)  
        image = Image.fromarray(all_masks, mode="L").resize((IMAGE_RESIZE, IMAGE_RESIZE), Image.NEAREST)        
        output = io.BytesIO()
        image.save(output, format='JPEG')
        return output.getvalue()
    raise Exception(type(rle_masks))
    
  
@pandas_udf("binary")
def computeMaskUDF(s: pd.Series) -> pd.Series:
  return s.apply(mask)
