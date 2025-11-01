import requests

def check_google_status():
    try:
        response = requests.get('https://status.google.com/', timeout=10)
        print(f"Google Status Page: {response.status_code}")
        
        # Проверяем конкретно Sheets API
        sheets_status = requests.get('https://sheets.googleapis.com/$discovery/rest?version=v4', timeout=10)
        print(f"Sheets API Status: {sheets_status.status_code}")
    except Exception as e:
        print(f"Ошибка проверки: {e}")

check_google_status()