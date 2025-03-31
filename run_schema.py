import oracledb
import json
import requests
from settings import ACTIVE_ENV, CONFIG_FILE, SCHEMA_FILE_URL
import os

def load_config(env):
    with open(CONFIG_FILE, "r") as file:
        config = json.load(file)
    return config[env]

def download_schema_sql():
    try:
        response = requests.get(SCHEMA_FILE_URL)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print("❌ Błąd pobierania pliku SQL z GitHuba:", e)
        return None

def execute_schema(sql_text, connection):
    try:
        cursor = connection.cursor()
        statements = sql_text.split(";")

        for statement in statements:
            statement = statement.strip()
            if statement:
                try:
                    cursor.execute(statement)
                except oracledb.DatabaseError as e:
                    print("⚠️ Błąd w zapytaniu:", e)
                    print("⛔ Pominięto fragment:\n", statement[:100], "...")
        connection.commit()
        print("✅ Skrypt SQL został wykonany.")
    except Exception as e:
        print("❌ Błąd wykonania skryptu SQL:", e)

def run_schema():
    cfg = load_config(ACTIVE_ENV)
    print(f"🔗 Połącz z bazą ({ACTIVE_ENV})...")

    try:
        with oracledb.connect(
            user=cfg["user"],
            password=cfg["password"],
            dsn=cfg["dsn"]
        ) as conn:
            print("✅ Połączenie udane.")

            sql_text = download_schema_sql()
            if sql_text:
                execute_schema(sql_text, conn)
    except oracledb.Error as e:
        print("❌ Błąd połączenia:", e)

if __name__ == "__main__":
    run_schema()
    print("🔄 Wykonano skrypt do aktualizacji schematu bazy danych.")