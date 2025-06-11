import json
import csv
import mysql.connector
from datetime import datetime
from dateutil import parser
import re

def normalize(name):
    """Нормалізує ім'я для порівняння"""
    return re.sub(r'\W+', '', name.lower().strip())

def load_config(path):
    """Завантажує конфігурацію клієнтів"""
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)["clients"]

def load_mapping(path):
    """Завантажує мапінг між оригінальним і 'чистим' ім'ям клієнта"""
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_sequence(path, mapping):
    """Завантажує правильний порядок клієнтів з CSV, використовуючи мапінг"""
    sequence = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_name = row.get("Client", "").strip()
            mapped_name = mapping.get(raw_name)
            if mapped_name:
                sequence.append(normalize(mapped_name))
            else:
                print(f"[WARNING] No mapping found for client: {raw_name}")
    return sequence

def fetch_last_payment_task(client):
    """Отримує останню дату оновлення для payment-звітів"""
    try:
        conn = mysql.connector.connect(
            host=client["host"],
            port=client["port"],
            user=client["user"],
            password=client["password"],
            database=client["database"]
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT * FROM task_count tc 
            WHERE tc.code LIKE '%payment%'
            ORDER BY tc.updated_at DESC
            LIMIT 1
        """)
        row = cursor.fetchone()

        if row:
            raw_date = row["updated_at"]
            try:
                parsed_date = parser.parse(raw_date)
                formatted_date = parsed_date.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            except Exception as e:
                print(f"[WARNING] Can't parse date: {raw_date}")
                formatted_date = raw_date

            return {
                "client": client["name"],
                "code": row["code"],
                "updated_at": formatted_date,
                "status": row.get("status", ""),
            }
        else:
            return {
                "client": client["name"],
                "code": "No Data",
                "updated_at": "N/A",
                "status": ""
            }

    except Exception as e:
        print(f"[ERROR] {client['name']}: {e}")
        return {
            "client": client["name"],
            "code": "ERROR",
            "updated_at": "ERROR",
            "status": ""
        }
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

def main():
    clients = load_config(r"C:\Users\Dell\PycharmProjects\Work\config.json")
    mapping = load_mapping(r"C:\Users\Dell\PycharmProjects\Work\client_mapping.json")
    sequence = load_sequence(r"C:\Users\Dell\PycharmProjects\Work\order.csv", mapping)

    results = []
    for client in clients:
        print(f"🔄 Processing: {client['name']}")
        result = fetch_last_payment_task(client)
        results.append(result)
        print(f"✅ {result['client']} → Last payment code: {result['code']} at {result['updated_at']}")

    def sort_key(entry):
        normalized_name = normalize(mapping.get(entry["client"], entry["client"]))
        if normalized_name in sequence:
            return sequence.index(normalized_name)
        return float('inf')  # якщо клієнт не знайдений — в кінець

    results_sorted = sorted(results, key=sort_key)

    with open("last_payment_tasks.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Client", "Code", "Updated At", "Status"])
        for r in results_sorted:
            writer.writerow([r["client"], r["code"], r["updated_at"], r["status"]])

    print("\n📁 Data written to 'last_payment_tasks.csv'")

if __name__ == "__main__":
    main()
