import os
import shutil
from datetime import datetime

# Função para organizar o arquivo order_details.csv
def organize_order_details(source_dir = '/opt/airflow/data', order_details_file = 'order_details.csv', today_date = datetime.now().strftime('%Y-%m-%d')):
    src_path = os.path.join(source_dir, order_details_file)
    print(src_path)
    if os.path.exists(src_path):
        dest_dir = os.path.join(source_dir, 'csv', today_date)
        os.makedirs(dest_dir, exist_ok=True)
        shutil.move(src_path, os.path.join(dest_dir, order_details_file))
        print(f"'{order_details_file}' movido para {dest_dir}")
    else:   
        print(f"'{order_details_file}' não encontrado!")

