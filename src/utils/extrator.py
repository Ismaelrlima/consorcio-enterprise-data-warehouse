def extrair_primeira_lista(obj):
    
    if isinstance(obj, list):
        return obj
    
    if isinstance(obj, dict):
        for value in obj.values():
            result = extract_first_list(value)
            if isinstance(result, list):
                return result

    return None

#Dar uma estudada