import os
import pandas as pd


class PaymentProcessor:
    def init(self, folder_path):
        """
        Ініціалізує клас для обробки файлів, які містять колонку 'Amount'.

        :param folder_path: Шлях до папки з CSV-файлами.
        """
        self.folder_path = folder_path

    def process_files(self):
        """
        Обробляє всі CSV-файли у вказаній папці.
        Якщо файл містить колонку 'Amount', обчислюється сума значень.

        :return: Словник з результатами обробки (назва файлу -> результат).
        """
        results = {}
        for file_name in os.listdir(self.folder_path):
            if file_name.endswith(".csv"):
                file_path = os.path.join(self.folder_path, file_name)
                print(f"Обробка файлу: {file_name}")

                # Обробляємо файл
                result = self.process_file(file_path)
                results[file_name] = result

        return results

    def process_file(self, file_path):
        """
        Обробляє один CSV-файл.
        Якщо файл містить колонку 'Amount', обчислюється сума значень.

        :param file_path: Шлях до файлу.
        :return: Результат обробки (сума Amount) або повідомлення про помилку.
        """
        try:
            # Зчитуємо CSV-файл
            df = pd.read_csv(file_path)

            # Перевіряємо, чи існує колонка "Amount"
            if "Amount" not in df.columns:
                return {"error": "Колонка 'Amount' не знайдена."}

            print("Дані перед очищенням:")
            print(df["Amount"])

            # Очищаємо колонку Amount
            try:
                df["Amount"] = (
                    df["Amount"]
                    .astype(str)  # Перетворюємо на рядки
                    .str.strip()  # Видаляємо пробіли на початку/кінці
                    .str.replace(r"^\$", "", regex=True)  # Видаляємо $ лише на початку рядка
                    .str.replace(",", "", regex=True)  # Видаляємо коми
                    .replace("", "0")  # Замінюємо порожні значення на "0"
                    .replace("nan", "0")  # Замінюємо NaN на "0"
                    .replace("N/A", "0")  # Замінюємо N/A на "0"
                )

                print("Дані після очищення:")
                print(df["Amount"])

                # Перетворюємо на числа
                df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)

                print("Дані після перетворення на числа:")
                print(df["Amount"])

            except ValueError as e:
                return {"error": f"Помилка при очищенні даних: {e}"}

            # Обчислюємо загальну суму
            total_Amount = df["Amount"].sum()

            # Повертаємо результат
            return {"total_Amount": total_Amount}

        except Exception as e:
            # У разі помилки повертаємо повідомлення про помилку
            return {"error": f"Помилка обробки файлу: {e}"}

# Приклад використання
if __name__ == "main":
    folder_path = "reports"  # Замініть на шлях до вашої папки
    processor = PaymentProcessor(folder_path)
    results = processor.process_files()

    # Виводимо результати
    for file_name, result in results.items():
        print(f"Файл: {file_name}")
        if "error" in result:
            print(f"  Помилка: {result['error']}")
        else:
            print(f"  Загальна сума: ${result['total_Amount']:.2f}")
