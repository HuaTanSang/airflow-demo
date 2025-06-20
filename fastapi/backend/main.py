from fastapi import FastAPI, UploadFile, File, Request
from PIL import Image
import numpy as np
import pickle
from io import BytesIO
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

app = FastAPI()
templates = Jinja2Templates(directory="templates")

with open("model/model.pkl", "rb") as f:
    model = pickle.load(f)

@app.get("/", response_class=HTMLResponse)
def landing_page(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

def preprocess_image(file):
    image = Image.open(BytesIO(file)).convert("RGB")
    image = image.resize((64, 64)) 
    arr = np.array(image) / 255.0
    return arr.flatten().reshape(1, -1)

@app.post("/predict")
async def predict(file: UploadFile = File(...)):
    contents = await file.read()
    vector = preprocess_image(contents)
    pred = model.predict(vector)[0]
    label = "Chó" if pred == 0 else "Mèo"
    return {"prediction": label}
