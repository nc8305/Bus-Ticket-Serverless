import azure.functions as func
import logging
import json
import os
from pymongo import MongoClient

# Khởi tạo Azure Function App
app = func.FunctionApp()

# Khởi tạo kết nối đến MongoDB Atlas (Chỉ khởi tạo 1 lần khi container bật lên)
# Các biến môi trường này sau này sẽ cấu hình trên giao diện Azure
MONGO_URI = os.environ.get("MONGODB_URI", "mongodb+srv://<user>:<pass>@cluster.mongodb.net/")
client = MongoClient(MONGO_URI)
db = client["bus_booking_db"]
collection = db["tickets"]

# Lắng nghe sự kiện từ Azure Service Bus (Hàng đợi)
@app.service_bus_queue_trigger(
    arg_name="azservicebus", 
    queue_name="ticket-queue", 
    connection="SERVICE_BUS_CONNECTION_STRING" # Tên biến môi trường chứa khóa kết nối
)
def ticket_processing_worker(azservicebus: func.ServiceBusMessage):
    # 1. Rút tin nhắn từ Queue ra
    message_body = azservicebus.get_body().decode('utf-8')
    logging.info(f"Nhận được giao dịch mới: {message_body}")

    try:
        # 2. Parse JSON
        ticket_data = json.loads(message_body)
        
        # 3. Lưu vào MongoDB
        # Trong Serverless, không cần lo bulk insert, cứ insert thẳng vì DB Cloud tự scale
        result = collection.insert_one(ticket_data)
        
        logging.info(f"✅ Đã lưu vé thành công với ID: {result.inserted_id}")
        
    except Exception as e:
        logging.error(f"❌ Lỗi xử lý vé: {e}")
        # Bắn lỗi ra ngoài. Service Bus sẽ tự động biết là hàm chạy thất bại
        # và đẩy tin nhắn này sang Dead-Letter Queue (DLQ) để tránh mất data.
        raise e
