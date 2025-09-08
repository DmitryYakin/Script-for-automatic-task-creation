import requests
import threading
from concurrent.futures import ThreadPoolExecutor
import time

import logging

logging.basicConfig(
    level=logging.INFO,
    filename='request_sender.log',      # Создаст `request_sender.log` с логом пациентов/статусов/оценок
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s'
)

API_AUTH_URL = "https://cloud.webiomed.ru/gate/api/v1/accounts/login/"
API_POST_URL = "https://cloud.webiomed.ru/dhra/api/v3/webiomedmpi/requests/"
USERNAME = "dagestan_data"
PASSWORD = "q5Dh9s4j/!|l"
NUM_THREADS = 5     # 40 должно быть норм, на 50 уже падат ошибки изредка
FILE_PATH = 'prom_patientid_for_update.csv'
gen_lock = threading.Lock()

CHUNK_SIZE = 100

REQ502 = []
REQ400 = []
REQ201 = []
REQ500 = []

write_locks = {
    201: threading.Lock(),
    400: threading.Lock(),
    500: threading.Lock(),
    502: threading.Lock(),
    424: threading.Lock(),
}

buffers = {
    201: [],
    400: [],
    500: [],
    502: [],
    424: [],
}

def data_generator_from_file(path: str):
    with open(path, "r") as f:
        for line in f:
            patient_id = line.strip()
            if patient_id:
                yield {
                    "medical_data": {},
                    "patient_id": int(patient_id),
                    "result_content": 2,
                    "use_case": "analysis_refer",
                    "evidence_level": "A",
                    "medical_care_condition": 1
                }

data_gen = data_generator_from_file(FILE_PATH)

def get_next_data():
    with gen_lock:
        try:
            return next(data_gen)
        except StopIteration:
            return None

def get_auth_token():
    response = requests.post(API_AUTH_URL, json={
        "username": USERNAME,
        "password": PASSWORD
    }, headers={"Content-Type": "application/json"})
    response.raise_for_status()
    token = response.json().get("token")
    if not token:
        raise Exception
    return token

def flush_buffer(status_code: int):
    """Write buffer to file and clear it."""
    file_name = f"{status_code}.txt"
    with write_locks[status_code]:
        if buffers[status_code]:
            with open(file_name, "a") as f:
                for patient_id in buffers[status_code]:
                    f.write(f"{patient_id}\n")
            buffers[status_code].clear()

def handle_status(status_code: int, patient_id: int):
    if status_code in buffers:
        buffers[status_code].append(patient_id)
        if len(buffers[status_code]) >= CHUNK_SIZE:
            flush_buffer(status_code)

def worker(session: requests.Session, token: str, thread_id: int):
    while True:
        item = get_next_data()
        if item is None:
            print(f"[Thread-{thread_id}] No more data.")
            break

        headers = {"Authorization": f"JWT {token}"}
        try:
            response = session.post(API_POST_URL, json=item, headers=headers)
            status_code = response.status_code
            handle_status(status_code, item["patient_id"])
            req_id = None
            if response.status_code == 201:
                req_id = response.json().get("id")
            logging.info(f"[Thread-{thread_id}] Sent patient_id={item['patient_id']} | Status: {status_code}; Request ID: {req_id}")
            print(f"[Thread-{thread_id}] Sent patient_id={item['patient_id']} | Status: {status_code}; Request ID: {req_id}")
        except Exception as e:
            logging.info(f"[Thread-{thread_id}] Error sending patient_id={item['patient_id']}: {e}")
            print(f"[Thread-{thread_id}] Error sending patient_id={item['patient_id']}: {e}")
        time.sleep(0.1)

def flush_all_buffers():
    for code in buffers:
        flush_buffer(code)

def main():
    print("Getting token...")
    logging.info("Getting token...")
    token = get_auth_token()
    print("Token OK")
    logging.info("Token OK")
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            for thread_id in range(NUM_THREADS):
                executor.submit(worker, session, token, thread_id + 1)
    flush_all_buffers()

main()

# Python 3.8
# Будут созданы еще txt файлы (400.txt, 500.txt, 502.txt) с пациентами, на оценки которых ошибки
# 201.txt - с успешными пациентами. По окончанию можно скопировать всех пациентов из 400.txt, 500.txt, 502.txt и подкинуть
# в целевой csv, чтобы закинуть оценку еще раз. Зачастую, 400 - ответ по мертвым пациентам, их можно не закидывать в целевой файл.
# После этого почистить 400.txt, 500.txt, 502.txt, в них будет дозапись в следующей итерации.
# Формат целевого файла - просто id-шники пациентов на каждой новой строке:
# 51800048
# 47150693
# 47138073
# 47123251
# 47167030
# 47212902
# 47204185
# 47175797
# ...
