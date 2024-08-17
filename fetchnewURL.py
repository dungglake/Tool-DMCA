import logging
from mysqlConnect import get_mysql_connection
from google_sheets import get_gspread_client, get_urls_from_google_sheets

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_new_urls_from_google_sheets(credentials_file, spreadsheet_id, sheet_name):
    try:
        with get_mysql_connection() as conn:
            cursor = conn.cursor()
            client = get_gspread_client(credentials_file)

            # Lấy các URL hiện có trong cơ sở dữ liệu
            cursor.execute("SELECT url FROM urls")
            existing_urls = {url[0] for url in cursor.fetchall()}
            
            # Lấy các URL mới từ Google Sheets
            logging.info(f"Fetching URLs from Google Sheets with Spreadsheet ID: {spreadsheet_id}, Sheet Name: {sheet_name}")
            new_urls = get_urls_from_google_sheets(client, spreadsheet_id, sheet_name)
            
            if not new_urls:
                logging.info("No URLs found in Google Sheets.")
                return

            urls_to_insert = [(url, None) for url in new_urls if url not in existing_urls]  # Thêm None cho cột trạng thái để đặt thành NULL

            if urls_to_insert:
                logging.info(f"Tìm thấy {len(urls_to_insert)} URL mới từ Google Sheets.")
                cursor.executemany("INSERT INTO urls (url, status) VALUES (%s, %s)", urls_to_insert)
                conn.commit()
            else:
                logging.info("Không tìm thấy URL mới từ Google Sheets.")
                
    except Exception as e:
        logging.error(f"Lỗi khi kết nối cơ sở dữ liệu hoặc chèn dữ liệu: {e}")
    return
