import logging
from mysqlConnect import get_mysql_connection
from checkDMCA import check_and_update_dmca_status
from process_unavailable import process_unavailable_urls
from google_sheets import get_gspread_client, update_google_sheet_status

def process_pending_urls(credentials_file, spreadsheet_id, sheet_name):
    try:
        # Khởi tạo client Google Sheets
        client = get_gspread_client(credentials_file)
        logging.info("Google Sheets client initialized successfully.")

        with get_mysql_connection() as conn:
            cursor = conn.cursor()
            
            # Lấy các URL có trạng thái "Pending" hoặc "Unavailable" từ database
            cursor.execute("SELECT id, url, status FROM urls WHERE status IN ('Pending', 'Unavailable', 'None')")
            urls = cursor.fetchall()
            logging.info(f"Fetched {len(urls)} URLs from database.")
            
            url_status_map = {}
            for url_id, url, current_status in urls:
                try:
                    # Kiểm tra DMCA cho từng URL và cập nhật trạng thái
                    new_status = check_and_update_dmca_status(url)  # Cập nhật với đủ tham số
                    logging.info(f"URL '{url}' checked. New status: {new_status}")

                    if new_status == "Unavailable":
                        retry_count = 0
                        while retry_count < 3:
                            try:
                                # Gọi hàm để nộp DMCA và cập nhật lại trạng thái
                                new_status = process_unavailable_urls(url)
                                logging.info(f"URL '{url}' processed for DMCA. New status: {new_status}")
                                
                                # Cập nhật lại trạng thái trong cơ sở dữ liệu sau khi nộp DMCA
                                cursor.execute("UPDATE urls SET status = %s WHERE id = %s", (new_status, url_id))
                                url_status_map[url] = new_status
                                break  # Thành công, thoát khỏi vòng lặp
                            except Exception as e:
                                retry_count += 1
                                logging.error(f" [!] Lỗi khi xử lý URL '{url}' (ID: {url_id}) - Lần thử {retry_count}")
                                logging.error(e)

                        if retry_count == 3:
                            logging.error(f" [!] Thất bại sau 3 lần thử với URL '{url}' (ID: {url_id})")
                            logging.error(f"Có lỗi xảy ra khi xử lý URL: {url} (ID: {url_id})")
                    else:
                        # Cập nhật trạng thái trong database nếu không phải là "Unavailable"
                        cursor.execute("UPDATE urls SET status = %s WHERE id = %s", (new_status, url_id))
                        url_status_map[url] = new_status

                except Exception as e:
                    logging.error(f"Lỗi khi kiểm tra URL '{url}': {e}")
            conn.commit()
            logging.info("Database updated successfully.")

            # Cập nhật trạng thái lên Google Sheets
            if url_status_map:
                logging.info("Updating Google Sheets with new statuses.")
                update_google_sheet_status(client, spreadsheet_id, sheet_name, url_status_map)
            
    except Exception as e:
        logging.error(f"Lỗi khi xử lý URL chờ xử lý: {e}")
