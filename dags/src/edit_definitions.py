import json
import re

def update_paths_with_date(json_file_path, new_date):
    try:
        # Carregar o arquivo JSON
        with open(json_file_path, 'r') as file:
            data = json.load(file)

        # Regex para capturar datas no formato YYYY-MM-DD
        date_pattern = r"\d{4}-\d{2}-\d{2}"

        # Atualizar os caminhos substituindo a data antiga pela nova
        for entry in data:
            entry['path'] = re.sub(date_pattern, new_date, entry['path'])

        # Salvar o JSON atualizado
        with open(json_file_path, 'w') as file:
            json.dump(data, file, indent=2)

        print(f"Updated paths in {json_file_path} with the new date: {new_date}")
    
    except Exception as e:
        print("An error occurred:", e)
