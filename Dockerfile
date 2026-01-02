# Use a lightweight Python base image
FROM python:3.10-slim

# 1. Install System Dependencies (Crucial for OCR & PDF)
# poppler-utils -> for pdf2image
# tesseract-ocr -> for pytesseract
# tesseract-ocr-jpn -> Japanese language pack
RUN apt-get update && apt-get install -y \
    poppler-utils \
    tesseract-ocr \
    tesseract-ocr-jpn \
    libgl1 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# 2. Copy and install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy the rest of the app code
COPY . .

# 4. Run the app with a long timeout (OCR is slow!)
# We bind to port 10000 which Render expects
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:10000", "--timeout", "120"]
