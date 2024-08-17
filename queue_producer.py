import pika
from mysqlConnect import get_mysql_connection

def producer(db_lock):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Khai báo queue
    channel.queue_declare(queue='dmca_queue', durable=True)

    # Lấy kết nối MySQL từ hàm get_mysql_connection
    conn = get_mysql_connection()

    try:
        with db_lock:  # Thêm khóa tại đây để bảo vệ truy cập cơ sở dữ liệu
            with conn.cursor(buffered=True) as cursor:  # Sử dụng buffered=True để đảm bảo tất cả kết quả được đọc
                # Truy vấn SQL lấy url_id và url có status là NULL
                cursor.execute("SELECT id, url FROM urls WHERE status IS NULL")

                url_count = 0

                # Lặp qua từng URL lấy được và đưa vào queue
                for row in cursor:
                    if len(row) != 2:
                        raise ValueError(f"Expected 2 values, got {len(row)}: {row}")
                    url_id, url = row 
                    message = f"{url_id},{url}"
                    channel.basic_publish(exchange='', routing_key='dmca_queue', body=message, properties=pika.BasicProperties(delivery_mode=2,  # Làm cho tin nhắn bền vững
                        ))
                    print(f"Đã đưa URL '{url}' vào hàng đợi với ID '{url_id}'")
                    url_count += 1 

                # Cập nhật trạng thái URL thành "None" để tránh thêm lại vào hàng đợi
                cursor.execute("UPDATE urls SET status = 'None' WHERE status IS NULL")
                conn.commit() 

        print(f"Tổng số URL đã được đưa vào hàng đợi: {url_count}")
    finally:
        conn.close()  # Đảm bảo kết nối MySQL được đóng sau khi sử dụng

    # Đóng kết nối RabbitMQ
    connection.close()
