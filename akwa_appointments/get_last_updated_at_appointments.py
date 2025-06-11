import json
import csv
import mysql.connector
from datetime import datetime


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
            client_original = row.get("Client", "").strip()
            if client_original in mapping:
                sequence.append(mapping[client_original])
            else:
                print(f"[WARNING] No mapping found for client: {client_original}")
    return sequence


def fetch_last_charge_task(client):
    """Отримує останню дату оновлення для charge-звітів"""
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
            WHERE tc.code LIKE '%appointment%'
            ORDER BY tc.updated_at DESC
            LIMIT 1
        """)
        row = cursor.fetchone()

        if row:
            raw_date = row["updated_at"]

            if isinstance(raw_date, str):
                try:
                    parsed_date = datetime.strptime(raw_date, "%H:%M.%S")
                    formatted_date = parsed_date.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                except ValueError:
                    formatted_date = raw_date
            else:
                formatted_date = raw_date.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if raw_date else "N/A"

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
    clients = load_config("../config.json")
    mapping = load_mapping("../client_mapping.json")
    sequence = load_sequence("../order.csv", mapping)

    results = []
    for client in clients:
        print(f"🔄 Processing: {client['name']}")
        result = fetch_last_charge_task(client)
        results.append(result)
        print(f"✅ {result['client']} → Last code: {result['code']} at {result['updated_at']}")

    def sort_key(x):
        clean_name = mapping.get(x["client"], "")
        if clean_name in sequence:
            return sequence.index(clean_name)
        else:
            return float('inf')  # невідомі клієнти — в кінець

    results_sorted = sorted(results, key=sort_key)

    with open("last_charge_tasks.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Client", "Code", "Updated At", "Status"])
        for r in results_sorted:
            writer.writerow([
                r["client"],
                r["code"],
                r["updated_at"],
                r["status"]
            ])

    print("\n📁 Data written to 'last_charge_tasks.csv'")


if __name__ == "__main__":
    main()