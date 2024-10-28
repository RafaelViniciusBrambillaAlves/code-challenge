import os
import shutil
from datetime import datetime

# Função para organizar arquivos com prefixo 'public-'
def organize_public_files(source_dir='/opt/airflow/data', public_prefix='public-', today_date=datetime.now().strftime('%Y-%m-%d')):
    files_moved = False  # Variável para controlar se algum arquivo foi encontrado
    for filename in os.listdir(source_dir):
        if filename.startswith(public_prefix):
            # Extrai a categoria do nome do arquivo
            category = filename[len(public_prefix):].split('.')[0]
            dest_dir = os.path.join(source_dir, 'postgres', category, today_date)
            os.makedirs(dest_dir, exist_ok=True)
            
            src_path = os.path.join(source_dir, filename)
            
            if os.path.exists(src_path):
                shutil.move(src_path, os.path.join(dest_dir, filename))
                print(f"'{filename}' movido para {dest_dir}")
                files_moved = True
            else:
                print(f"'{filename}' não encontrado!")

    if not files_moved:
        print(f"Nenhum arquivo com o prefixo '{public_prefix}' encontrado em {source_dir}.")

