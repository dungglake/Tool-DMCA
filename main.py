import threading
import schedule
import time
import logging
from queue_producer import producer
from queue_consumer import consumer
from fetchnewURL import fetch_new_urls_from_google_sheets
from checknewDMCA import process_pending_urls

logging.basicConfig(level=logging.INFO)

credentials_file = r'selenium_test\ultimate-choir-423207-b4-efc6afc87101.json'  # Đường dẫn tới tệp thông tin xác thực
spreadsheet_id = "1zKQBXi_GEYd-xXhrRyKeX70m80OhUd_pyainM9y4V1o"  # ID của Google Sheets
sheet_name = "List viết mới DMCA - Dũng"  # Định rõ phạm vi dữ liệu

# Biến boolean để theo dõi trạng thái hoàn thành công việc và khóa
task_completed = False
db_lock = threading.Lock()

def schedule_job():
    global task_completed
    logging.info("Running scheduled job...")
    with db_lock:
        try:
            # Fetch các URL mới từ Google Sheets và thêm vào cơ sở dữ liệu
            fetch_new_urls_from_google_sheets(credentials_file, spreadsheet_id, sheet_name)
            process_pending_urls(credentials_file, spreadsheet_id, sheet_name)
            logging.info("Scheduled job completed successfully.")
        except Exception as e:
            logging.error(f"Error in scheduled job: {e}")

    task_completed = True

def run_schedule():
    # Hàm này sẽ chạy các công việc đã lên lịch
    while True:
        schedule.run_pending()
        time.sleep(1)

def main():
    global task_completed
    
    # Lên lịch chạy job mỗi giờ
    schedule.every(1).hours.do(schedule_job)

    # Khởi động producer và consumer trong các luồng riêng biệt
    producer_thread = threading.Thread(target=producer, args=(db_lock,))
    consumer_thread = threading.Thread(target=consumer, args=(db_lock, credentials_file, spreadsheet_id, sheet_name))
    
    producer_thread.start()
    consumer_thread.start()
    
    logging.info("Producer and consumer threads started.")
    
    # Chạy schedule trong một luồng riêng biệt
    schedule_thread = threading.Thread(target=run_schedule)
    schedule_thread.start()
    logging.info("Schedule thread started.")

if __name__ == "__main__":
    main()
