import azure.functions as func
import logging
import json
import os
from pymongo import MongoClient

app = func.FunctionApp()

# Khởi tạo kết nối MongoDB
MONGO_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
try:
    client = MongoClient(MONGO_URI)
    db = client["bus_booking_db"]
    collection = db["tickets"]
except Exception as e:
    logging.error(f"Khong the ket noi MongoDB: {e}")

# =====================================================================
# HÀM 1: ENTRY POINT (HTTP -> Service Bus)
# Hứng 50.000 requests từ Load Tester và đẩy vảo hàng đợi
# =====================================================================


@app.route(route="ticket_trigger", auth_level=func.AuthLevel.ANONYMOUS)
@app.service_bus_queue_output(
    arg_name="msg",
    connection="SERVICE_BUS_CONNECTION_STRING",
    queue_name="ticket-queue",
)
def http_to_service_bus(req: func.HttpRequest, msg: func.Out[str]) -> func.HttpResponse:
    try:
        # Nhận cục JSON từ Load Tester
        req_body = req.get_json()

        # Đẩy thẳng cục JSON đó vào Service Bus
        msg.set(json.dumps(req_body))

        return func.HttpResponse(
            "Đã ghi nhận yêu cầu đặt vé vào hàng đợi!",
            status_code=202,  # HTTP 202: Đã tiếp nhận nhưng chưa xử lý xong
        )
    except ValueError:
        return func.HttpResponse("Payload không hợp lệ.", status_code=400)


# =====================================================================
# HÀM 2: WORKER (Service Bus -> MongoDB)
# Tự động kéo tin nhắn từ hàng đợi ra để lưu vào DB
# =====================================================================


@app.service_bus_queue_trigger(
    arg_name="azservicebus",
    queue_name="ticket-queue",
    connection="SERVICE_BUS_CONNECTION_STRING",
)
def ticket_processing_worker(azservicebus: func.ServiceBusMessage):
    message_body = azservicebus.get_body().decode("utf-8")
    try:
        ticket_data = json.loads(message_body)
        collection.insert_one(ticket_data)
        logging.info(f"✅ Đã lưu vé: {ticket_data.get('user_id')}")
    except Exception as e:
        logging.error(f"❌ Lỗi xử lý vé: {e}")
        raise e
