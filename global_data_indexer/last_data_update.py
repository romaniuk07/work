import mysql.connector
import csv
import json


def load_config(path="global_config.json"):
    """Load database configuration from JSON file"""
    with open(path, "r") as f:
        return json.load(f)["database"]


def run_query(cursor):
    """Execute SQL query and return results and headers"""
    query = """
    SELECT 
        parent.id AS Source_id,
        parent.name AS Parent_Name,

        MAX(CASE WHEN charges.id IS NOT NULL THEN task_charge.updated_at END) AS Last_Charge_Task_Update,
        MAX(CASE WHEN payments.id IS NOT NULL THEN task_payment.updated_at END) AS Last_Payment_Task_Update,
        MAX(CASE WHEN appointments.id IS NOT NULL THEN task_appointment.updated_at END) AS Last_Appointment_Task_Update

    FROM sources AS parent

    -- Appointments
    LEFT JOIN sources AS child_appointments 
        ON child_appointments.parent_id = parent.id
        AND child_appointments.type NOT IN ('N/A')
    LEFT JOIN appointments 
        ON appointments.source_id = child_appointments.id
        AND appointments.appointment_date >= CURDATE() - INTERVAL 30 DAY
    LEFT JOIN task_count AS task_appointment
        ON task_appointment.source_id = parent.id
        AND task_appointment.code LIKE '%charge%'
        AND task_appointment.code LIKE '%appointment%'

    -- Payments
    LEFT JOIN sources AS child_payments 
        ON child_payments.parent_id = parent.id
        AND child_payments.type NOT IN ('N/A')
    LEFT JOIN payments 
        ON payments.source_id = child_payments.id
        AND payments.payment_date >= CURDATE() - INTERVAL 30 DAY
    LEFT JOIN task_count AS task_payment
        ON task_payment.source_id = parent.id
        AND task_payment.code LIKE '%charge%'
        AND task_payment.code LIKE '%payment%'

    -- Charges
    LEFT JOIN sources AS child_charges 
        ON child_charges.parent_id = parent.id
        AND child_charges.type NOT IN ('N/A', 'magento', 'alle', 'liine', 'klara', 'aspire', 'Internal')  
    LEFT JOIN charges 
        ON charges.source_id = child_charges.id  
        AND COALESCE(
            charges.charge_input_date,
            charges.bill_date,
            charges.payment_date,
            charges.charge_service_date
        ) >= CURDATE() - INTERVAL 30 DAY
    LEFT JOIN task_count AS task_charge
        ON task_charge.source_id = parent.id
        AND task_charge.code LIKE '%charge%'

    WHERE parent.type IN ('akwa', 'akwa_disable')

    GROUP BY parent.id, parent.name

    ORDER BY parent.name;
    """

    cursor.execute(query)
    return cursor.fetchall(), cursor.description


def write_to_csv(data, headers, filename="last_task_update.csv"):
    """Write results to CSV file"""
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([desc[0] for desc in headers])  # Column headers
        writer.writerows(data)  # Data rows

    print(f"\n✅ Results saved to: {filename}")


def main():
    config = load_config()

    try:
        print("🔄 Connecting to database...")
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        print("🔍 Executing SQL query...")
        data, headers = run_query(cursor)

        print("📊 Writing data to CSV...")
        write_to_csv(data, headers)

    except Exception as e:
        print(f"[❌ Error]: {e}")
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass


if __name__ == "__main__":
    main()