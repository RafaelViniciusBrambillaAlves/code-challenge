import os

def check_date_directory(date_param, **context):
    # Diretório base onde as pastas são esperadas
    base_dir = "/opt/airflow/data/csv/"
    # Diretório completo a ser verificado
    date_directory = os.path.join(base_dir, date_param)
    
    # Verifica se o diretório existe
    if not os.path.exists(date_directory):
        raise FileNotFoundError(f"Pasta correspondente à data {date_param} não encontrada em {base_dir}.")
    else:
        print(f"Pasta encontrada: {date_directory}. Prosseguindo com o processo.")
