import streamlit as st
import requests
from PIL import Image
import io

st.set_page_config(page_title="🐶🐱 Phân loại ảnh Chó / Mèo", layout="centered")

st.title("🐶🐱 Dự đoán ảnh Chó / Mèo bằng mô hình KNN")
st.markdown("Tải ảnh lên và mô hình sẽ dự đoán đó là **chó** hay **mèo**.")

# FastAPI endpoint
FASTAPI_URL = "http://localhost:8000/predict" 

uploaded_file = st.file_uploader("Chọn ảnh ", type=["jpg", "jpeg", "png"])

if uploaded_file:
    # Hiển thị ảnh 
    image = Image.open(uploaded_file).convert("RGB")
    st.image(image, caption="Ảnh bạn đã tải lên", use_column_width=True)

    if st.button("🚀 Dự đoán"):
        with st.spinner("Đang gửi ảnh đến mô hình..."):
            try:
                # Gửi ảnh đến FastAPI
                files = {"file": (uploaded_file.name, uploaded_file.getvalue(), uploaded_file.type)}
                response = requests.post(FASTAPI_URL, files=files)

                if response.status_code == 200:
                    result = response.json()
                    prediction = result.get("prediction")

                    # Hiển thị kết quả
                    if prediction.lower() == "chó" or prediction.lower() == "dog":
                        st.success("Mô hình dự đoán: **CHÓ**")
                    elif prediction.lower() == "mèo" or prediction.lower() == "cat":
                        st.success("Mô hình dự đoán: **MÈO**")
                    else:
                        st.warning(f"Kết quả không xác định: {prediction}")
                else:
                    st.error(f"Lỗi server: {response.status_code}")
            except Exception as e:
                st.error(f"Không thể kết nối đến server: {e}")
