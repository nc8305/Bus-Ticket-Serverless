import asyncio
import aiohttp
import time
import random
import json
import argparse
import os
from dotenv import load_dotenv
from aiokafka import AIOKafkaProducer

# Hàm sinh dữ liệu dùng chung (có thể dùng cho cả GPS hoặc Ticket)
def generate_payload(index, data_type="ticket"):
    if data_type == "gps":
        return {
            "vehicle_id": f"bus_SG_{index % 500}",
            "route_id": f"route_{random.randint(1, 100)}",
            "latitude": 10.762622 + random.uniform(-0.05, 0.05),
            "longitude": 106.660172 + random.uniform(-0.05, 0.05),
            "timestamp": time.time(),
            "speed": random.randint(0, 60)
        }
    else: # Ticket
        return {
            "user_id": f"student_{index}",
            "bus_route_id": f"route_{random.randint(1, 150)}",
            "timestamp": time.time(),
            "ticket_quantity": random.randint(1, 4)
        }

# Van xả áp: Giữ cho máy không bị sập khi mở quá nhiều luồng mạng
sem = asyncio.Semaphore(2000)

async def send_http_request(session, url, payload):
    async with sem:
        try:
            async with session.post(url, json=payload) as response:
                return response.status
        except Exception as e:
            return str(e)

async def send_kafka_request(producer, topic_name, payload):
    async with sem:
        try:
            message = json.dumps(payload).encode('utf-8')
            await producer.send_and_wait(topic_name, message)
            return "KAFKA_SUCCESS"
        except Exception as e:
            return str(e)

async def run_serverless(total_requests, url, data_type):
    print(f"🚀 [SERVERLESS] Đang nã {total_requests} requests lên Azure (HTTP)...")
    start_time = time.time()
    
    async with aiohttp.ClientSession() as session:
        tasks = [
            asyncio.create_task(send_http_request(session, url, generate_payload(i, data_type)))
            for i in range(total_requests)
        ]
        results = await asyncio.gather(*tasks)
        
    end_time = time.time()
    success_count = results.count(200) + results.count(202) + results.count(201)
    print(f"📊 [SERVERLESS] Kết quả: {success_count}/{total_requests} thành công | Thời gian: {end_time - start_time:.2f} giây")

async def run_server(total_requests, kafka_bootstrap, topic_name, data_type):
    print(f"🚀 [SERVER] Đang nã {total_requests} records vào Kafka (TCP)...")
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    start_time = time.time()
    
    try:
        tasks = [
            asyncio.create_task(send_kafka_request(producer, topic_name, generate_payload(i, data_type)))
            for i in range(total_requests)
        ]
        results = await asyncio.gather(*tasks)
    finally:
        await producer.stop()
        
    end_time = time.time()
    success_count = results.count("KAFKA_SUCCESS")
    print(f"📊 [SERVER] Kết quả: {success_count}/{total_requests} thành công | Thời gian: {end_time - start_time:.2f} giây")

async def main():
    parser = argparse.ArgumentParser(description="Dual Load Tester: Hỗ trợ Server (Kafka) và Serverless (HTTP)")
    parser.add_argument('--mode', type=str, choices=['server', 'serverless', 'both'], default='both',
                        help='Chế độ chạy: server, serverless, hoặc both')
    parser.add_argument('--requests', type=int, default=50000, help='Số lượng request/record cần nã')
    parser.add_argument('--type', type=str, choices=['ticket', 'gps'], default='ticket', help='Loại dữ liệu sinh ra')
    
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    # Cấu hình endpoints
    HTTP_URL = os.getenv("AZURE_API_URL", "https://<LINK_AZURE_API_CUA_BAN_SAU_NAY>")
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", 'localhost:9092')
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", 'bus-gps-tracking')
    
    print(f"🔥 BẮT ĐẦU CHẠY CHẾ ĐỘ: {args.mode.upper()} | Số lượng: {args.requests} | Loại data: {args.type}")
    
    tasks = []
    # Khởi chạy các luồng bắn theo chế độ được chọn
    if args.mode in ['serverless', 'both']:
        tasks.append(run_serverless(args.requests, HTTP_URL, args.type))
    if args.mode in ['server', 'both']:
        tasks.append(run_server(args.requests, KAFKA_BOOTSTRAP, KAFKA_TOPIC, args.type))
        
    # Chạy song song cả hai nếu mode="both"
    await asyncio.gather(*tasks)
    print("\n✅ ĐÃ HOÀN THÀNH TOÀN BỘ TEST!")

if __name__ == "__main__":
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
