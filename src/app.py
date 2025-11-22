import os
import time
import logging
import re
import pandas as pd
from O365 import Account
from dotenv import load_dotenv
from datetime import datetime
from bs4 import BeautifulSoup
from io import StringIO

# --- 1. PATHS & LOGGING SETUP ---
current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_dir)

INVENTORY_FILE = os.path.join(project_root, 'data', 'inventory_synced.xlsx')
LOG_FILE = os.path.join(project_root, 'logs', 'bot.log')
ENV_FILE = os.path.join(project_root, '.env')
TEMP_DIR = os.path.join(project_root, 'data', 'temp_attachments')

os.makedirs(TEMP_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# --- 2. CONFIGURATION ---
load_dotenv(ENV_FILE)

CLIENT_ID = os.getenv('AZURE_CLIENT_ID')
CLIENT_SECRET = os.getenv('AZURE_CLIENT_SECRET')
TENANT_ID = os.getenv('AZURE_TENANT_ID')
MONITORED_EMAIL = os.getenv('MONITORED_EMAIL')
SP_HOST = os.getenv('SHAREPOINT_HOST')
SP_SITE_PATH = os.getenv('SHAREPOINT_SITE_PATH')
SP_FILE_PATH = os.getenv('SHAREPOINT_FILE_PATH')

if not MONITORED_EMAIL:
    logging.warning("⚠️  WARNING: MONITORED_EMAIL is missing in .env.")

recipients_raw = os.getenv('TARGET_EMAILS')
if recipients_raw:
    TARGET_RECIPIENTS = [email.strip() for email in recipients_raw.split(',')]
else:
    logging.warning("No TARGET_EMAILS found. Reports will not be sent.")
    TARGET_RECIPIENTS = []

credentials = (CLIENT_ID, CLIENT_SECRET)

# --- 3. INTELLIGENT PARSING FUNCTIONS ---

def find_column_by_name(df, keywords):
    """Helper to find a column matching a list of keywords."""
    # Create a map of {upper_col_name: real_col_name}
    cols = {str(c).upper().strip(): c for c in df.columns}
    for kw in keywords:
        if kw in cols:
            return cols[kw]
    return None

def extract_data_from_text(text):
    """
    Parses text line-by-line to associate Parts with Quantities.
    Returns list of dicts: [{'part': 'ABC', 'req_qty': 2}, ...]
    """
    results = []
    current_block_parts = [] # Parts found since the last Qty line
    
    lines = text.splitlines()
    
    # Regex Filters
    qty_regex = re.compile(r'(?:QTY|QUANTITY|COUNT)[:\s]+(\d+)|(\d+)\s*(?:EA|EACH|PC|PCS)', re.IGNORECASE)
    explicit_part_regex = re.compile(r'(?:P/N|PN|PART|PART NO|PART NUMBER|ALT|ALTERNATE)[:\s]+([A-Z0-9\-\.]+)', re.IGNORECASE)
    
    ignore_tokens = {
        'QTY', 'REQ', 'UM', 'EA', 'EACH', 'DESC', 'DESCRIPTION', 'REV', 'DATE', 
        'SIGNED', 'DELIVERED', 'BY', 'AND', 'OR', 'TO', 'FROM', 'SUBJECT', 'SENT', 
        'CC', 'HI', 'HELLO', 'REGARDS', 'THANKS', 'BOLT', 'SCREW', 'WASHER', 'NUT', 
        'PIN', 'RIVET', 'COLLAR', 'BUSHING', 'SEAL', 'SUPPORT', 'BEARING', 'CLAMP',
        'SN', 'S/N', 'SERIAL', 'AWB', 'OUTBOUND', 'SHIPPING', 'COMPANY', 'ACCOUNT',
        'USED', 'NOTES', 'TRACKING', 'PHONE', 'FAX', 'NEEDED', 'ASSEMBLY', 'ASSY'
    }

    for line in lines:
        clean_line = line.strip()
        if not clean_line: continue

        # Context Filters (Skip headers/footers)
        upper_line = clean_line.upper()
        if upper_line.startswith(('AIRCRAFT:', 'REASON:', 'SUBJECT:', 'FROM:', 'TO:', 'SENT:', 'ADDRESS:', 'SHIP TO:')):
            continue

        # 1. Check for Quantity Line (e.g., "Qty: 2")
        qty_match = qty_regex.search(clean_line)
        if qty_match:
            # Extract number (group 1 or group 2 depending on regex match)
            qty_str = qty_match.group(1) or qty_match.group(2)
            try:
                found_qty = int(qty_str)
            except:
                found_qty = 1
            
            # Apply this Qty to all parts found in the current block
            for part in current_block_parts:
                results.append({'part': part, 'req_qty': found_qty})
            
            # Reset block (Assume next parts belong to next Qty)
            current_block_parts = []
            continue # Skip part scanning on this line if it was just a Qty line

        # 2. Scan for Parts
        line_parts = set()
        
        # A. Explicit matches (PN: 123)
        explicit = explicit_part_regex.findall(clean_line)
        for p in explicit:
            clean_p = p.upper().strip('.,:; ')
            if len(clean_p) > 2:
                line_parts.add(clean_p)

        # B. Implicit matches (Word scanning)
        tokens = clean_line.split()
        for token in tokens:
            t = token.upper().strip('.,:;()[]"')
            
            if len(t) < 3: continue
            if not any(c.isdigit() for c in t): continue 
            if re.match(r'^\d{3}-\d{3}-\d{4}$', t): continue 
            if re.match(r'^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$', t): continue 
            if re.match(r'^N\d', t): continue 
            if re.match(r'^\d+(ST|ND|RD|TH)$', t): continue 

            if t not in ignore_tokens:
                line_parts.add(t)
        
        # Add found parts to current pending block
        current_block_parts.extend(list(line_parts))

    # Cleanup: If parts remain at end with no Qty line, assume Qty 1
    for part in current_block_parts:
        results.append({'part': part, 'req_qty': 1})

    return results

def extract_data_from_email(message):
    """Master parser returning list of {'part': X, 'req_qty': Y}."""
    
    # --- 1. Attachments ---
    if message.has_attachments:
        for attachment in message.attachments:
            if attachment.name.lower().endswith(('.xlsx', '.xls')):
                try:
                    save_path = os.path.join(TEMP_DIR, attachment.name)
                    attachment.save(TEMP_DIR)
                    df = pd.read_excel(save_path)
                    
                    part_col = find_column_by_name(df, ['P/N', 'PN', 'PART', 'PART NUMBER', 'ITEM'])
                    qty_col = find_column_by_name(df, ['QTY', 'QUANTITY', 'REQ', 'QTY REQ'])
                    
                    if part_col:
                        extracted = []
                        for _, row in df.iterrows():
                            p = str(row[part_col]).strip().upper()
                            if p and p != 'NAN':
                                try:
                                    q = int(row[qty_col]) if qty_col else 1
                                except:
                                    q = 1
                                extracted.append({'part': p, 'req_qty': q})
                        
                        os.remove(save_path)
                        return extracted, f"Excel ({attachment.name})"
                except Exception as e:
                    logging.error(f"Excel Parse Error: {e}")

    # --- 2. HTML Tables ---
    if message.body:
        try:
            html_content = StringIO(message.body)
            dfs = pd.read_html(html_content)
            for df in dfs:
                # Standard Table
                part_col = find_column_by_name(df, ['P/N', 'PN', 'PART', 'PART NUMBER'])
                qty_col = find_column_by_name(df, ['QTY', 'QUANTITY'])
                
                # Promoted Header Table (First row is header)
                if not part_col and not df.empty:
                    df_promoted = df.copy()
                    df_promoted.columns = df.iloc[0]
                    df_promoted = df_promoted[1:]
                    part_col = find_column_by_name(df_promoted, ['P/N', 'PN', 'PART', 'PART NUMBER'])
                    qty_col = find_column_by_name(df_promoted, ['QTY', 'QUANTITY'])
                    df = df_promoted

                if part_col:
                    extracted = []
                    for _, row in df.iterrows():
                        p = str(row[part_col]).strip().upper()
                        if p and p != 'NAN':
                            try:
                                q = int(row[qty_col]) if qty_col else 1
                            except:
                                q = 1
                            extracted.append({'part': p, 'req_qty': q})
                    return extracted, "HTML Table"
        except:
            pass 

    # --- 3. Text Scanner ---
    try:
        soup = BeautifulSoup(message.body, "html.parser")
        body_text = soup.get_text(separator="\n")
    except:
        body_text = "" 

    full_text = f"{message.subject}\n{body_text}"
    return extract_data_from_text(full_text), "Text Scanner"

# --- 4. CORE LOGIC ---

def download_from_sharepoint(account):
    if not SP_HOST: return False
    try:
        site = account.sharepoint().get_site(SP_HOST, SP_SITE_PATH)
        drive = site.get_default_document_library()
        item = drive.get_item_by_path(SP_FILE_PATH)
        item.download(to_path=os.path.dirname(INVENTORY_FILE))
        return True
    except Exception as e:
        logging.error(f"SharePoint Sync Failed: {e}")
        return False

def load_inventory(account_for_sync=None):
    if account_for_sync: download_from_sharepoint(account_for_sync)
    
    target_file = None
    # Prefer Synced Excel
    if SP_FILE_PATH:
        sp_name = os.path.basename(SP_FILE_PATH)
        if os.path.exists(os.path.join(project_root, 'data', sp_name)):
            target_file = os.path.join(project_root, 'data', sp_name)
            
    if not target_file and os.path.exists(os.path.join(project_root, 'data', 'inventory.csv')):
        target_file = os.path.join(project_root, 'data', 'inventory.csv')

    if not target_file: return None

    try:
        # Read file (handle Excel sheet logic)
        if target_file.endswith('.xlsx') or target_file.endswith('.xls'):
            try:
                df = pd.read_excel(target_file, sheet_name='InventoryIndex')
            except:
                df = pd.read_excel(target_file)
        else:
            df = pd.read_csv(target_file)

        if df.empty: return None

        # Dynamic Header Detection (for tables starting on row 5 etc)
        header_idx = None
        for i in range(min(10, len(df))):
            vals = df.iloc[i].astype(str).str.upper().tolist()
            if any('PARTNUMBER' in x.replace(" ", "") for x in vals):
                header_idx = i
                break
        
        if header_idx is not None:
             if target_file.endswith('.xlsx'):
                 df = pd.read_excel(target_file, sheet_name='InventoryIndex', header=header_idx+1)

        df = df.rename(columns={
            'PartNumber': 'part_number', 'Qty': 'quantity', 'Condition': 'condition'
        })
        
        if 'part_number' not in df.columns: return None
        df['part_number'] = df['part_number'].astype(str).str.strip().str.upper()
        return df
    except Exception as e:
        logging.error(f"DB Read Error: {e}")
        return None

def generate_html_report(results, original_subject, method):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    html = f"""
    <div style="font-family: Arial, sans-serif; color: #333;">
        <h3>Inventory Check Report</h3>
        <p><strong>Triggered by:</strong> {original_subject}<br>
           <strong>Read Method:</strong> {method}<br>
           <strong>Time:</strong> {timestamp}</p>
        
        <table style="border-collapse: collapse; width: 100%; max-width: 900px;">
            <thead>
                <tr style="background-color: #0078D4; color: white; text-align: left;">
                    <th style="padding: 8px; border: 1px solid #ddd;">Part Number</th>
                    <th style="padding: 8px; border: 1px solid #ddd;">Status</th>
                    <th style="padding: 8px; border: 1px solid #ddd;">Condition</th>
                    <th style="padding: 8px; border: 1px solid #ddd;">Req Qty</th>
                    <th style="padding: 8px; border: 1px solid #ddd;">In Stock</th>
                    <th style="padding: 8px; border: 1px solid #ddd;">Remaining</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for row in results:
        bg_color = "#ffe6e6" if "MISSING" in row['status'] or "OUT OF STOCK" in row['status'] or "LOW STOCK" in row['status'] else "#ffffff"
        font_weight = "bold" if bg_color == "#ffe6e6" else "normal"
        
        html += f"""
            <tr style="background-color: {bg_color};">
                <td style="padding: 8px; border: 1px solid #ddd;">{row['part']}</td>
                <td style="padding: 8px; border: 1px solid #ddd; font-weight: {font_weight}">{row['status']}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{row['condition']}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{row['req_qty']}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{row['stock_qty']}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{row['remaining']}</td>
            </tr>
        """
    html += "</tbody></table></div>"
    return html

def process_emails():
    logging.info("Connecting to Azure...")
    try:
        account = Account(credentials, auth_flow_type='credentials', tenant_id=TENANT_ID)
        if not account.authenticate(): return
    except Exception as e:
        return

    df = load_inventory(account_for_sync=account)
    mailbox = account.mailbox(resource=MONITORED_EMAIL)
    inbox = mailbox.inbox_folder()

    try:
        messages = inbox.get_messages(limit=25, download_attachments=True)
    except: return

    for message in messages:
        if message.is_read: continue
        if "Inventory Alert" in message.subject:
            message.mark_as_read()
            continue
        if 'C CHECK' not in message.subject: continue
            
        logging.info(f"Processing Email: {message.subject}")
        
        # Get List of {'part': 'XYZ', 'req_qty': 5}
        items, method = extract_data_from_email(message)
        
        if not items:
            message.mark_as_read()
            continue

        if df is None: continue

        results = []
        for item in items:
            part = item['part']
            req_qty = item['req_qty']
            
            # Find Part in DB
            match = df[df['part_number'] == part]
            
            # Smart Lookup (StartsWith)
            if match.empty:
                match = df[df['part_number'].str.startswith(part, na=False)]
                if not match.empty:
                    part = f"{part} (Matched: {match.iloc[0]['part_number']})"

            if match.empty:
                results.append({
                    'part': part, 'status': 'MISSING (Unknown Part)', 
                    'condition': 'N/A', 'req_qty': req_qty, 'stock_qty': 0, 'remaining': 0
                })
            else:
                row = match.iloc[0]
                try: stock_qty = int(row['quantity'])
                except: stock_qty = 0
                condition = row.get('condition', 'N/A')
                if pd.isna(condition): condition = 'N/A'

                # --- LOGIC: Calculate Status ---
                if stock_qty == 0:
                    status = "OUT OF STOCK"
                    remaining = 0
                elif stock_qty >= req_qty:
                    status = "IN STOCK"
                    remaining = stock_qty - req_qty
                else:
                    status = f"LOW STOCK (Have {stock_qty})"
                    remaining = 0 # Or negative to show shortage? Usually 0 implies "None left after this"

                results.append({
                    'part': part, 'status': status, 'condition': condition, 
                    'req_qty': req_qty, 'stock_qty': stock_qty, 'remaining': remaining
                })

        if TARGET_RECIPIENTS:
            m = account.new_message(resource=MONITORED_EMAIL)
            m.to.add(TARGET_RECIPIENTS)
            m.subject = f"Inventory Alert: {message.subject}"
            m.body = generate_html_report(results, message.subject, method)
            m.send()
            logging.info(f"Report sent.")
        
        message.mark_as_read()
        logging.info("Email marked as read.")

if __name__ == "__main__":
    logging.info(f"System Online. Monitoring: {MONITORED_EMAIL}")
    try:
        while True:
            process_emails()
            time.sleep(60)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.critical(f"Crash: {e}")