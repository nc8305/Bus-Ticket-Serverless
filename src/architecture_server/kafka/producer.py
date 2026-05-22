import asyncio
import json
import time
import random
from aiokafka import AIOKafkaProducer


# Trả về đúng data GPS nguyên bản
def generate_gps_payload(vehicle_index):
    return {
        "vehicle_id": f"bus_SG_{vehicle_index % 500}",
        "route_id": f"route_{random.randint(1, 100)}",
        "latitude": 10.762622 + random.uniform(-0.05, 0.05),
        "longitude": 106.660172 + random.uniform(-0.05, 0.05),
        "timestamp": time.time(),
        "speed": random.randint(0, 60),
    }


# Van xả áp: giới hạn 2000 luồng chạy cùng lúc để Docker không ngộp thở
sem = asyncio.Semaphore(2000)


async def send_with_semaphore(producer, topic_name, message):
    async with sem:
        await producer.send_and_wait(topic_name, message)


async def fire_requests(producer, topic_name, total_requests):
    tasks = []
    print(f"Đang chuẩn bị {total_requests} tọa độ GPS...")

    for i in range(total_requests):
        payload = generate_gps_payload(i)
        message = json.dumps(payload).encode("utf-8")
        task = asyncio.create_task(send_with_semaphore(producer, topic_name, message))
        tasks.append(task)

    print("BẮT ĐẦU NÃ 50.000 TỌA ĐỘ VÀO KAFKA CÙNG LÚC!")
    await asyncio.gather(*tasks)


async def main():
    # Trả về đúng topic hệ thống cũ của bạn
    topic_name = "bus-gps-tracking"
    producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")

    await producer.start()
    start_time = time.time()

    try:
        await fire_requests(producer, topic_name, 50000)
    except Exception as e:
        print(f"Hệ thống crash với lỗi: {e}")
    finally:
        await producer.stop()

    end_time = time.time()
    print(
        f"\n[Kết quả Test] Hoàn thành đẩy 50.000 record "
        f"trong: {end_time - start_time:.2f} giây"
    )


if __name__ == "__main__":
    import sys

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
