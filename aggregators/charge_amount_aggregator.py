import os
import pandas as pd


class PaymentProcessor:
    def __init__(self, folder_path):
        self.folder_path = folder_path

    def process_files(self):
        results = {}
        for file_name in os.listdir(self.folder_path):
            if file_name.endswith(".csv"):
                file_path = os.path.join(self.folder_path, file_name)
                print(f"Обробка файлу: {file_name}")
                result = self.process_file(file_path)
                results[file_name] = result
        return results

    def process_file(self, file_path):
        try:
            df = pd.read_csv(file_path)

            if "TotalChargeAmount" not in df.columns:
                return {"error": "Колонка 'TotalChargeAmount' не знайдена."}

            print("Дані перед очищенням:")
            print(df["TotalChargeAmount"])

            try:
                df["TotalChargeAmount"] = (
                    df["TotalChargeAmount"]
                    .astype(str)
                    .str.strip()
                    .str.replace(r"^\$", "", regex=True)
                    .str.replace(",", "", regex=True)
                    .replace("", "0")
                    .replace("nan", "0")
                    .replace("N/A", "0")
                )

                print("Дані після очищення:")
                print(df["TotalChargeAmount"])

                df["TotalChargeAmount"] = pd.to_numeric(df["TotalChargeAmount"], errors="coerce").fillna(0)

                print("Дані після перетворення на числа:")
                print(df["TotalChargeAmount"])

            except ValueError as e:
                return {"error": f"Помилка при очищенні даних: {e}"}

            total_Amount = df["TotalChargeAmount"].sum()
            return {"total_Amount": total_Amount}

        except Exception as e:
            return {"error": f"Помилка обробки файлу: {e}"}


# Приклад використання
if __name__ == "__main__":
    folder_path = "reports"  # Замініть на шлях до вашої папки
    processor = PaymentProcessor(folder_path)
    results = processor.process_files()

    for file_name, result in results.items():
        print(f"Файл: {file_name}")
        if "error" in result:
            print(f"  Помилка: {result['error']}")
        else:
            print(f"  Загальна сума: ${result['total_Amount']:.2f}")
