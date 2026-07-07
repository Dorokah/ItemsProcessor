import io
import math
import time
from http import HTTPStatus

import requests
from PIL import Image, ImageFile
from src.utils import config_provider
ImageFile.LOAD_TRUNCATED_IMAGES = True

image_service_url = config_provider.get_image_service_url()
image_service_timeout = config_provider.get_image_timeout()
min_image_width = config_provider.get_image_minimum_width()
min_image_height = config_provider.get_image_minimum_height()


def get_image_and_metadata(algo_request):
    algo_request, image_bytes = get_image(algo_request)
    metadata, image_bytes = extract_image_metadata(image_bytes)
    validate_image_dimensions_fit_for_processing(metadata["imageHeight"], metadata["imageWidth"])
    return algo_request, image_bytes, metadata


def get_image(algo_request):
    image_url = get_image_url(algo_request)
    get_image_start_timestamp = time.time()
    response = requests.get(image_url,
                            stream=True,
                            timeout=float(image_service_timeout))
    algo_request['getImageDuration'] = time.time() - get_image_start_timestamp
    if response.status_code != HTTPStatus.OK:
        raise Exception("Got error from image service")
    return algo_request, response.content


def get_image_url(algo_request):
    if 'entityId' in algo_request:
        image_id = algo_request['entityId']
        return image_service_url.format(image_id)
    return algo_request['imageFullUrl']


def extract_image_metadata(image_bytes):
    pil_image = Image.open(io.BytesIO(image_bytes))
    img_type = pil_image.format
    img_width = pil_image.size[0]
    img_height = pil_image.size[1]
    img_size_kb = round(len(image_bytes) / 1024, 2)
    mode = pil_image.mode
    bands_amount = len(pil_image.getbands())
    sqrt_pixels_amount = math.sqrt(img_height * img_width)
    metadata = {'imageType': img_type,
                'imageSizeKB': img_size_kb,
                'imageWidth': img_width,
                'imageHeight': img_height,
                'imageBandsAmount': bands_amount,
                'imageMode': mode,
                'squaredPixelsAmount': sqrt_pixels_amount}
    return metadata, image_bytes


def validate_image_dimensions_fit_for_processing(height, width):
    if height < min_image_height or width < min_image_width:
        raise Exception("image dimension are to small for processing")
