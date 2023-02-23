import cv2
import layoutparser as lp
import pytesseract
import pandas as pd
import numpy as np


model = lp.models.Detectron2LayoutModel('lp://PubLayNet/faster_rcnn_R_50_FPN_3x/config',
                                        extra_config=["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8],)


def detect_ocr(pix):
    langs = 'eng'
    pix = cv2.imdecode(np.frombuffer(pix.tobytes('jpg'), np.uint8), -1)
    pix = cv2.resize(pix, None, fx=1.5, fy=1.5)
    pix = pix[..., ::-1]
    layout = model.detect(pix)
    text_blocks = lp.Layout([b for b in layout if b.type != 'Figure'])
    text_list = []
    for block in text_blocks:
        segment_image = (block
                         .pad(left=5, right=5, top=5, bottom=5)
                         .crop_image(pix))
        text = pytesseract.image_to_string(
            segment_image, lang=langs, config='--oem 1')
        text_list.append(text)
    return '\n'.join(text_list)
