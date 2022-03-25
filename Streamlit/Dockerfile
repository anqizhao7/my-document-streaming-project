FROM python:3.9
COPY requirements.txt /tmp/

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --requirement /tmp/requirements.txt

WORKDIR /streamlitapp
EXPOSE 8501
COPY . .
ENTRYPOINT [ "streamlit", "run" ]
CMD ["streamlitapp.py"]