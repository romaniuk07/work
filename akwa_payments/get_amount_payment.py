import json
import csv
import mysql.connector
import re

def load_config(path):
    with open(path, 'r') as f:
        return json.load(f)["clients"]

def load_order(path):
    with open(path, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Пропустити заголовок
        return [row[0] for row in reader]

def normalize(name):
    return re.sub(r'\W+', '', str(name).lower())

def fetch_payment_data(client):
    """Отримує суму payments за останні 30 днів"""
    try:
        conn = mysql.connector.connect(
            host=client["host"],
            port=client["port"],
            user=client["user"],
            password=client["password"],
            database=client["database"]
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ROUND(SUM(p.amount), 2)
            FROM payments p
            WHERE p.payment_date >= CURDATE() - INTERVAL 30 DAY;
        """)
        total = cursor.fetchone()[0] or 0.0

        cursor.close()
        conn.close()

        return total

    except Exception as e:
        print(f"[ERROR] {client['name']}: {e}")
        return "ERROR"

def main():
    clients = load_config("../config.json")
    client_order = load_order(r"C:\Users\Dell\PycharmProjects\Work\order.csv")  # Завантаження бажаного порядку
    normalized_order = [normalize(name) for name in client_order]

    # Створення мапи для впорядкування
    order_index = {normalize(name): i for i, name in enumerate(client_order)}

    output_data = []

    for client in clients:
        print(f"🔄 Processing: {client['name']}")
        total_amount = fetch_payment_data(client)
        norm_name = normalize(client["name"])
        output_data.append({
            "Client": client["name"],
            "Total": total_amount,
            "sort_index": order_index.get(norm_name, float("inf"))  # Якщо немає — в кінець
        })
        print(f"✅ {client['name']} → Total payments: {total_amount}")

    # Сортування за sort_index
    output_data.sort(key=lambda x: x["sort_index"])

    # Запис у CSV
    with open("list_of_akwa_payments_amount.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Client", "Total Payment Amount (Last 30 Days)"])
        for row in output_data:
            writer.writerow([row["Client"], row["Total"]])

    print("\n📁 Data written to 'list_of_akwa_payments_amount.csv'")

if __name__ == "__main__":
    main()
