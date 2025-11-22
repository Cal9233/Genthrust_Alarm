import os
import logging
from O365 import Account
from dotenv import load_dotenv

# Load environment variables
current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_dir)
load_dotenv(os.path.join(project_root, '.env'))

# Enable extensive logging to see the raw HTTP response
logging.basicConfig(level=logging.DEBUG)

client_id = os.getenv('AZURE_CLIENT_ID')
client_secret = os.getenv('AZURE_CLIENT_SECRET')
tenant_id = os.getenv('AZURE_TENANT_ID')

print(f"\n--- AUTHENTICATION DEBUGGER ---")
print(f"Tenant ID:     {tenant_id}")
print(f"Client ID:     {client_id}")
# Print first 3 chars to verify it's not the ID (Secrets usually start with special chars or letters)
print(f"Secret starts: {client_secret[:3]}...") 
print(f"-------------------------------\n")

credentials = (client_id, client_secret)
account = Account(credentials, auth_flow_type='credentials', tenant_id=tenant_id)

print("Attempting to authenticate...")
try:
    result = account.authenticate()
    if result:
        print("\n✅ SUCCESS! Authentication worked.")
    else:
        print("\n❌ FAILURE. The library returned False.")
except Exception as e:
    print("\n❌ CRASHED. Here is the exact error from Microsoft:")
    print(e)