import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import logging

def get_gspread_client(credentials_file):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_file(credentials_file, scopes=scopes)
    client = gspread.authorize(credentials)
    return client

def get_urls_from_google_sheets(client, spreadsheet_id, sheet_name):
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    values = sheet.get_all_values()
    
    if len(values) > 0:
        # Chuyển đổi dữ liệu sang DataFrame
        df = pd.DataFrame(values)  # Không giả định hàng đầu tiên là tiêu đề cột
        urls = df.iloc[:, 0].tolist()  # Giả sử URL nằm trong cột A
        return urls
    else:
        logging.info("No data found in the specified range.")
        return []

def update_google_sheet_status(client, spreadsheet_id, sheet_name, url_status_map):
    try:
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
        values = sheet.get_all_values()

        # In ra giá trị hiện tại để kiểm tra
        for value in values:
            logging.info(value)
        
        # Tìm hàng chứa URL và cập nhật trạng thái
        updated_values = []
        for row_index, row in enumerate(values):
            url = row[0]
            if url in url_status_map:
                if len(row) == 1:
                    row.append(url_status_map[url])  # Thêm trạng thái vào cột B nếu chưa tồn tại
                else:
                    row[1] = url_status_map[url]  # Cập nhật trạng thái vào cột B nếu đã tồn tại
            updated_values.append(row)

        # In ra giá trị mới để kiểm tra
        for value in updated_values:
            logging.info(value)
        
         # Cập nhật lại Google Sheets với toàn bộ dữ liệu
        range_to_update = f'A1:B{len(updated_values)}'
        sheet.update(range_to_update, updated_values)
        logging.info("Google Sheets updated successfully.")
    except Exception as e:
        logging.error(f"Lỗi khi cập nhật Google Sheets: {e}")
