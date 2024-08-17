import pika
import logging
import time
from checkDMCA import check_and_update_dmca_status
from process_unavailable import process_unavailable_urls
from mysqlConnect import get_mysql_connection
from google_sheets import update_google_sheet_status, get_gspread_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_rabbitmq():
    parameters = pika.ConnectionParameters('localhost')
    return pika.BlockingConnection(parameters)

def consumer(db_lock, credentials_file, spreadsheet_id, sheet_name):
    while True:
        try:
            # Kết nối RabbitMQ
            connection = connect_to_rabbitmq()
            channel = connection.channel()
            channel.queue_declare(queue='dmca_queue', durable=True)

            def callback(ch, method, properties, body):
                url_info = body.decode()
                # Giả sử url_info chứa 'url_id,url' dưới dạng chuỗi và bạn cần tách nó ra
                try:
                    url_id, url = url_info.split(',')
                except ValueError:
                    logging.error(f"Invalid message format: {url_info}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                logging.info(f"Đang xử lý URL: {url} với ID: {url_id}")

                with db_lock:  # Thêm khóa tại đây
                    # Kiểm tra trạng thái DMCA
                    try:
                        status_text = check_and_update_dmca_status(url)
                        logging.info(f"URL '{url}' checked. Status: {status_text}")
                    except Exception as e:
                        logging.error(f"Lỗi khi kiểm tra trạng thái DMCA: {e}")
                        status_text = "Unavailable"

                    # Cập nhật trạng thái trong cơ sở dữ liệu
                    try:
                        with get_mysql_connection() as conn:
                            cursor = conn.cursor()
                            cursor.execute("UPDATE urls SET status = %s WHERE id = %s", (status_text, url_id))
                            conn.commit()
                            logging.info(f"Database updated for URL '{url}' with status '{status_text}'.")
                    except Exception as e:
                        logging.error(f"Lỗi khi cập nhật trạng thái trong cơ sở dữ liệu: {e}")

                    # Lấy lại trạng thái từ cơ sở dữ liệu để đảm bảo tính nhất quán
                    try:
                        with get_mysql_connection() as conn:
                            cursor = conn.cursor()
                            cursor.execute("SELECT status FROM urls WHERE id = %s", (url_id,))
                            status_row = cursor.fetchone()
                            if status_row:
                                status_text = status_row[0]
                            else:
                                status_text = "Unavailable"
                        logging.info(f"Trạng thái DMCA của URL '{url}' (ID: {url_id}): {status_text}")
                    except Exception as e:
                        logging.error(f"Lỗi khi lấy trạng thái từ cơ sở dữ liệu: {e}")
                        status_text = "Unavailable"

                    if status_text in ["Active", "Pending"]:
                        logging.info(f"URL '{url}' (ID: {url_id}) đang ở trạng thái {status_text}. Bỏ qua nộp DMCA.")
                    else:
                        # Nếu URL không ở trạng thái "Active" hoặc "Pending", thực hiện quy trình nộp DMCA
                        retry_count = 0
                        while retry_count < 3:
                            try:
                                # Gọi hàm để nộp DMCA và cập nhật lại trạng thái
                                status_text = process_unavailable_urls(url)
                                
                                # Cập nhật lại trạng thái trong cơ sở dữ liệu sau khi nộp DMCA
                                with get_mysql_connection() as conn:
                                    cursor = conn.cursor()
                                    cursor.execute("UPDATE urls SET status = %s WHERE id = %s", (status_text, url_id))
                                    conn.commit()

                                logging.info(f"Đã xử lý DMCA cho URL '{url}' (ID: {url_id}) - Trạng thái: {status_text}")
                                break  # Thành công, thoát khỏi vòng lặp
                            except Exception as e:
                                retry_count += 1
                                logging.error(f" [!] Lỗi khi xử lý URL '{url}' (ID: {url_id}) - Lần thử {retry_count}")
                                logging.error(e)

                        if retry_count == 3:
                            logging.error(f" [!] Thất bại sau 3 lần thử với URL '{url}' (ID: {url_id})")
                            logging.error(f"Có lỗi xảy ra khi xử lý URL: {url} (ID: {url_id})")

                    # Khởi tạo client Google Sheets
                    try:
                        client = get_gspread_client(credentials_file)
                        url_status_map = {url: status_text if status_text is not None else "Unavailable"}
                        logging.info(f"Updating Google Sheets with status for URL '{url}': {status_text}")
                        update_google_sheet_status(client, spreadsheet_id, sheet_name, url_status_map)
                    except Exception as e:
                        logging.error(f"Lỗi khi cập nhật Google Sheets: {e}")

                ch.basic_ack(delivery_tag=method.delivery_tag)  # Xác nhận đã xử lý xong

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue='dmca_queue', on_message_callback=callback, auto_ack=False)

            logging.info(' [*] Đang chờ URL. Để thoát, nhấn CTRL+C')
            channel.start_consuming()
        except (pika.exceptions.StreamLostError, pika.exceptions.AMQPConnectionError) as e:
            logging.error(f"Connection lost: {e}")
            time.sleep(5)  # Chờ một thời gian trước khi thử lại
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            break
