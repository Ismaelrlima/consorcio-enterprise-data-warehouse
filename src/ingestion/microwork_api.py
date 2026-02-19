import requests
import os
from dotenv import load_dotenv

load_dotenv()

def buscar_dados_microwork(body: dict) -> dict:

    url = os.getenv('API_URL')
    token = os.getenv('API_TOKEN')

    headers = {
        "Content-Type" : "application/json",
        "Authorization" : f"Bearer {token}"
    }

    if not url:
        raise ValueError('URL não encontrada')
    if not token:
        raise ValueError('Token não encontrado')

    response = requests.post(url, headers=headers, json=body, timeout=30)
    response.raise_for_status()
    
    if response.status_code == 200:
        print('Dados puxados com sucesso')
        return response.json()
    
    if response.status_code != 200:
        raise ValueError('Não foi possivel puxar dados da api')
        return None