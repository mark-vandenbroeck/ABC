from database_pg import get_db_connection
import sys

def test_secure_connection():
    try:
        print("Attempting to connect with SSL certificates...")
        conn = get_db_connection()
        print("Connection successful!")
        
        # Check SSL status
        if conn.info.ssl_in_use:
            print("Verified: Connection is using SSL.")
            print(f"Protocol: {conn.info.ssl_attribute('protocol')}")
            print(f"Cipher: {conn.info.ssl_attribute('key_bits')} bits, {conn.info.ssl_attribute('cipher')}")
        else:
            print("WARNING: Connection is NOT using SSL!")
            
        conn.close()
        sys.exit(0)
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    test_secure_connection()
