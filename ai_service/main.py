import psycopg2
import time

def main():
    "
    Placeholder for future functionality. Currently, the service connects to the database
    but does not perform any operations.
    "
    try:
        conn = psycopg2.connect(
            dbname="leadsdb",
            user="postgres",
            password="postgres",
            host="db"
            
        )
        print("Connected to the database successfully.")
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
    finally:
        try:
            conn.close()
            print("Database connection closed.")
        except:
            pass

if __name__ == "__main__":
    print("Service running... (No active features)")
    main()
    
    while True:
        time.sleep(60)
