import base64
import concurrent.futures
import csv
import json
import logging
import os
import random
import re
import sys
import threading
import time
import traceback
from io import BytesIO
from pathlib import Path
from selenium.webdriver.chrome.options import Options
import psutil
import pyautogui
import pytesseract
import requests
from PIL import Image
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException, ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QTextEdit, \
    QPushButton, QLineEdit, QComboBox, QMessageBox
from PyQt5.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView
from PyQt5.QtWidgets import QApplication, QMainWindow

csv_lock = threading.Lock()
file_lock = threading.Lock()
browser_ready_event = threading.Event()
batch_completion_event = threading.Event()
stop_event = threading.Event()
active_browsers = []
browser_status = {}
transaction_status = {}
status_lock = threading.Lock()
total_browsers = 0

import requests
import webbrowser
from PyQt5.QtWidgets import QMessageBox

# KHAI BÁO PHIÊN BẢN HIỆN TẠI CỦA ỨNG DỤNG
CURRENT_VERSION = "1.0.0"

# Cấu hình logging
logging.basicConfig(level=logging.INFO, filename="roulette.log", format="%(asctime)s - %(levelname)s - %(message)s")

# Đường dẫn cơ bản
if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "config.json"
CONGNAP_FILE = BASE_DIR / "congnap.txt"

# Định nghĩa API URL và headers
url_payments_sms = 'http://localhost:8088/api/payments/sms-otp-auto'
url_payments_smotp = 'http://localhost:8088/api/payments'
headers = {
    'Content-Type': 'application/json',
    'Cookie': 'NP_25089888=642abbb08e234264ab02018caeead96d'
}


class ProxyHandler:
    def __init__(self, api_key):
        self.api_key = api_key

    def get_current_ip(self, proxy):
        """Lấy IP hiện tại của proxy"""
        try:
            response = requests.get('http://ipinfo.io/ip', proxies={"http": proxy, "https": proxy}, timeout=10)
            if response.status_code == 200:
                return response.text.strip()
            return None
        except Exception as e:
            logging.error(f"Lỗi khi lấy IP cho proxy {proxy}: {e}")
            return None

    def change_ip_using_api(self, proxy, retry_limit=3):
        """Đổi IP và kiểm tra xem IP đã thay đổi chưa"""
        url = f"https://app.proxyno1.com/api/change-key-ip/{self.api_key}"
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        initial_ip = self.get_current_ip(proxy)
        logging.info(f"IP ban đầu của proxy {proxy}: {initial_ip}")

        retry_count = 0
        while retry_count < retry_limit:
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    if data.get('status', 1) == 0:
                        time.sleep(10)  # Chờ API xử lý
                        new_ip = self.get_current_ip(proxy)
                        if new_ip and new_ip != initial_ip:
                            logging.info(f"Đổi IP thành công cho proxy {proxy}: {initial_ip} -> {new_ip}")
                            return True, new_ip
                        else:
                            logging.warning(f"IP không thay đổi cho proxy {proxy}: {new_ip}")
                            retry_count += 1
                            if retry_count < retry_limit:
                                match = re.search(r'Đợi (\d+) giây', data.get('message', ''))
                                wait_time = int(match.group(1)) if match else 15
                                logging.info(f"Đợi {wait_time} giây trước khi thử lại đổi IP cho proxy {proxy}")
                                time.sleep(wait_time)
                    else:
                        retry_count += 1
                        if retry_count < retry_limit:
                            match = re.search(r'Đợi (\d+) giây', data.get('message', ''))
                            wait_time = int(match.group(1)) if match else 15
                            logging.info(f"Đợi {wait_time} giây trước khi thử lại đổi IP cho proxy {proxy}")
                            time.sleep(wait_time)
                else:
                    retry_count += 1
                    if retry_count < retry_limit:
                        logging.warning(f"Mã trạng thái {response.status_code} cho proxy {proxy}, đợi 15 giây")
                        time.sleep(15)
            except Exception as e:
                logging.error(f"Lỗi khi đổi IP cho proxy {proxy}: {e}")
                retry_count += 1
                if retry_count < retry_limit:
                    time.sleep(15)
        logging.error(f"Không thể đổi IP cho proxy {proxy} sau {retry_limit} lần thử")
        return False, initial_ip

operating_systems = ['Windows NT 10.0; Win64; x64', 'Windows NT 11.0; Win64; x64']
trinh_duyet = ['Safari', 'Chrome']

def random_version():
    return f"{random.randint(133, 136)}.{random.randint(0, 9)}.{random.randint(0, 9)}.{random.randint(0, 9)}"

def random_safari_version():
    major_version = random.randint(14, 15)
    minor_version = random.randint(0, 2)
    patch_version = random.randint(0, 2)
    webkit_version = f"605.1.{random.randint(1, 9)}"
    return f"{major_version}.{minor_version}.{patch_version}/{webkit_version}"

browsers = {
    'Chrome': [random_version() for _ in range(10)],
    'Safari': [random_safari_version() for _ in range(10)],
}

def generate_random_user_agent():
    os_ = random.choice(operating_systems)
    browser_name = random.choice(trinh_duyet)
    browser_version = random.choice(browsers[browser_name])
    if browser_name == 'Chrome':
        return f'Mozilla/5.0 ({os_}) AppleWebKit/537.36 (KHTML, like Gecko) {browser_name}/{browser_version} Safari/537.36'
    elif browser_name == 'Safari':
        version, safari = browser_version.split('/')
        return f'Mozilla/5.0 ({os_}) AppleWebKit/{safari} (KHTML, like Gecko) Version/{version} Safari/{safari}'

def check_proxy(proxy, proxy_handler, max_retries=3):
    retry_count = 0
    while retry_count < max_retries:
        try:
            response = requests.get('https://www.google.com', proxies={"http": proxy, "https": proxy}, timeout=20)
            if response.status_code == 200:
                current_ip = proxy_handler.get_current_ip(proxy)
                logging.info(f"Proxy {proxy} hoạt động với IP: {current_ip}")
                return True
            else:
                raise Exception(f"Mã trạng thái không phải 200: {response.status_code}")
        except Exception as e:
            logging.warning(f"Lỗi kiểm tra proxy {proxy} (lần {retry_count + 1}/{max_retries}): {e}")
            retry_count += 1
            if retry_count >= max_retries:
                return False
            success, _ = proxy_handler.change_ip_using_api(proxy, retry_limit=1)
            if not success:
                return False
            time.sleep(5)
    return False

def decode_base64_to_image(base64_str):
    image_data = base64.b64decode(base64_str)
    image_buffer = BytesIO(image_data)
    return Image.open(image_buffer)

def demo_imagetotext(image):
    text = pytesseract.image_to_string(image)
    text = text.replace('/', '').replace("\n", '')
    return text[:4]

def handle_captcha(driver):
    try:
        captcha_input = driver.find_element(By.CSS_SELECTOR, 'input[ng-model="$ctrl.code"]')
        if captcha_input:
            captcha_input.click()
            time.sleep(1)
            captcha_image = driver.find_element(By.CSS_SELECTOR, 'img[src^="data:image/png;base64,"]')
            if captcha_image:
                captcha_src = captcha_image.get_attribute("src")
                if captcha_src and captcha_src.startswith("data:image/png;base64,"):
                    base64_str = captcha_src.split(",")[1]
                    image = decode_base64_to_image(base64_str)
                    captcha_code = demo_imagetotext(image)
                    if not captcha_code:
                        captcha_code = '1'
                    captcha_input.send_keys(captcha_code)
                    time.sleep(0.5)
                    return captcha_code
    except Exception:
        return None


def transfer_money_api(recvCustBankAcc, recvBankcode, transAmount, transContent, phone, type=0, api_mode='SMOTP'):
    url_payments = url_payments_sms if api_mode == 'SMS' else url_payments_smotp
    payload_dict = {
        "phone": str(phone),
        "recvCustBankAcc": str(recvCustBankAcc),
        "recvBankcode": str(recvBankcode),
        "transAmount": int(transAmount),
        "transContent": str(transContent),
        "type": type,
    }
    if api_mode == 'SMS':
        payload_dict["moneySourceBankCode"] = "VTT"
        payload_dict["bankCode"] = "VTT"

    try:
        raw_json_data = json.dumps(payload_dict)
        response = requests.post(url=url_payments, headers=headers, data=raw_json_data)
        response.raise_for_status()
        data_response = response.json()
        is_success = data_response.get('success', False)
        message = data_response.get('message', 'Không có thông báo')
        logging.info(f"Kết quả API cho {recvCustBankAcc}: success={is_success}, message={message}")
        return is_success, message
    except requests.exceptions.RequestException as e:
        logging.error(f"Lỗi khi gọi API: {e}")
        return False, f"Lỗi gọi API: {e}"
    except ValueError as e:
        logging.error(f"Lỗi phân tích JSON từ API: {e}")
        return False, f"Phản hồi JSON không hợp lệ: {e}"
    except KeyError as e:
        logging.error(f"Phản hồi API thiếu khóa {e}")
        return False, f"Thiếu khóa trong phản hồi: {e}"

def get_successful_accounts():
    successful_accounts = set()
    txt_filename = "nap.txt"  # Thay đổi từ nap_<current_date>.txt thành nap.txt
    try:
        with open(txt_filename, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('|')
                if len(parts) >= 6 and parts[5].strip() == "Thành công":
                    successful_accounts.add(parts[0].strip())
        logging.info(f"Tìm thấy {len(successful_accounts)} tài khoản thành công trong {txt_filename}")
    except FileNotFoundError:
        logging.info(f"{txt_filename} không tồn tại, coi như không có tài khoản thành công.")
    except Exception as e:
        logging.error(f"Lỗi khi đọc {txt_filename}: {e}")
    return successful_accounts

def write_to_csv(account_number, account_holder, transfer_amount, bank_name, transfer_content, transaction_status, transaction_time, phone_number, overwrite=False):
    csv_data = [
        f'="{account_number}"',
        account_holder,
        transfer_amount,
        bank_name,
        transfer_content,
        transaction_status,
        transaction_time,
        f'="{phone_number}"' if phone_number else ""
    ]
    with csv_lock:
        mode = 'w' if overwrite else 'a'
        with open('nap_tien.csv', mode, newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if overwrite or f.tell() == 0:
                writer.writerow(['SO TAI KHOAN', 'CHU TAI KHOAN', 'SO TIEN', 'NGAN HANG', 'NOI DUNG', 'TRANG THAI', 'THOI GIAN', 'SDT'])
            writer.writerow(csv_data)
        logging.info(f"Đã ghi thông tin vào nap_tien.csv: {csv_data}")
    return csv_data

import re
import time
import logging
import csv
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import threading

csv_lock = threading.Lock()
response_lock = threading.Lock()

def JDB_BCB(driver, link, username, password, mode, amount, phone_number, worker, api_mode, overwrite=False):
    global transaction_status, status_lock
    max_retries = 3
    retry_delay = 5
    max_api_retries = 3
    api_retry_delay = 10
    try:
        base_url = re.match(r'^(https?://[^/]+\.[a-zA-Z]{2,})', link).group(1)
        driver.get(base_url + "/Deposit")
        logging.info(f"Đã truy cập: {base_url}/Deposit cho tài khoản {username}")
        ip = worker.get_current_ip()
        worker.status_updated.emit(username, "Đang tải trang nạp tiền", ip)
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'h2[translate="Shared_NewsInfo_Title"].ng-scope'))
            )
            driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
            time.sleep(1)
            logging.info(f"Đã đóng popup nếu có cho tài khoản {username}.")
        except Exception:
            pass

        txt_filename = "nap.txt"

        if mode == "Không nạp":
            logging.info(f"Chế độ: Không nạp cho tài khoản {username}")
            time.sleep(10)
            transaction_status_val = "Thành công"
            transaction_time = time.strftime("%Y-%m-%d %H:%M:%S")
            csv_data = write_to_csv("", "", "", "", "", transaction_status_val, transaction_time, phone_number, overwrite)
            txt_data = f"{username}||||{transaction_time}|{transaction_status_val}"
            with open(txt_filename, 'a', encoding='utf-8') as f:
                f.write(txt_data + "\n")
            logging.info(f"Đã ghi thông tin vào {txt_filename}: {txt_data}")
            ip = worker.get_current_ip()
            worker.status_updated.emit(username, "Giao dịch thành công (Không nạp)", ip)
            with status_lock:
                transaction_status[username] = True
            return True

        elif mode == "VTP":
            logging.info(f"Chế độ: VTP cho tài khoản {username}")
            try:
                vtpay_selector = 'li[ng-repeat="item in $ctrl.payments"] span[translate="OnlineDeposit_VTPay"]'
                WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, vtpay_selector))).click()
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Đã chọn VTPay", ip)
                amount_input = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'input[ng-model="$ctrl.form.amount.value"]')))
                amount_input.clear()
                amount_input.send_keys(amount)
                logging.info(f"Đã điền số tiền: {amount} cho tài khoản {username}")
                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'span[translate="OnlineDeposit_Pay_Immediately"]'))).click()
                logging.info(f"Đã nhấp nút nạp tiền cho tài khoản {username}.")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Đã gửi yêu cầu nạp tiền VTPay", ip)
            except Exception as e:
                ip = worker.get_current_ip()
                logging.error(f"Lỗi khi xử lý VTP cho tài khoản {username}: {e}")
                worker.status_updated.emit(username, f"Lỗi VTP: {str(e)}", ip)
                with status_lock:
                    transaction_status[username] = False
                return False

        elif mode == "QR":
            logging.info(f"Chế độ: QR cho tài khoản {username}")
            def attempt_select_payment_method():
                attempt = 0
                selector = 'li[ng-repeat="item in $ctrl.payments"] span[translate="OnlineDeposit_BankScan"]'
                while attempt < max_retries:
                    try:
                        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector))).click()
                        ip = worker.get_current_ip()
                        worker.status_updated.emit(username, "Đã chọn QR", ip)
                        logging.info(f"Đã chọn phương thức QR cho tài khoản {username}.")
                        return True
                    except Exception:
                        payment_items = WebDriverWait(driver, 10).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'li[ng-repeat="item in $ctrl.payments"]')))
                        target_element = None
                        for item in payment_items:
                            try:
                                span = item.find_element(By.CSS_SELECTOR, 'span[translate="OnlineDeposit_BankScan"]')
                                if span:
                                    target_element = item
                                    break
                            except:
                                continue
                        if target_element:
                            driver.execute_script("arguments[0].scrollIntoView(true);", target_element)
                            WebDriverWait(driver, 10).until(EC.element_to_be_clickable(target_element)).click()
                            ip = worker.get_current_ip()
                            worker.status_updated.emit(username, "Đã chọn QR", ip)
                            logging.info(f"Đã chọn phương thức QR (kiểm tra danh sách) cho tài khoản {username}.")
                            return True
                        attempt += 1
                        if attempt < max_retries:
                            time.sleep(retry_delay)
                    except Exception as e:
                        logging.error(f"Lỗi chọn phương thức QR (lần thử {attempt + 1}/{max_retries}) cho tài khoản {username}: {e}")
                        attempt += 1
                        if attempt < max_retries:
                            time.sleep(retry_delay)
                logging.error(f"Không tìm thấy phương thức QR sau {max_retries} lần thử cho tài khoản {username}.")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Lỗi: Không chọn được QR", ip)
                return False

            def attempt_select_payment_option():
                attempt = 0
                # Đọc target_texts từ file congnap.txt
                target_texts = []
                try:
                    with open(CONGNAP_FILE, 'r', encoding='utf-8') as f:
                        target_texts = [line.strip() for line in f if line.strip()]
                    logging.info(f"Đã đọc {len(target_texts)} mục từ congnap.txt cho tài khoản {username}")
                except FileNotFoundError:
                    logging.error(f"Không tìm thấy file congnap.txt, sử dụng danh sách mặc định cho tài khoản {username}")
                    target_texts = [
                        "NOHUPAY 1- Nhập đúng nội dung",
                        "GO99PAY 1 - Nhập đúng nội dung",
                        "TT88 PAY 1-Nhập đúng nội dung",
                        "789PAY 6 - Nhập đúng nội dung",
                        "MMOOPAY 6 - Nhập đúng nội dung",
                        "10 ~ 300,000"
                    ]
                except Exception as e:
                    logging.error(f"Lỗi khi đọc congnap.txt: {e} cho tài khoản {username}")
                    target_texts = [
                        "NOHUPAY 1- Nhập đúng nội dung",
                        "GO99PAY 1 - Nhập đúng nội dung",
                        "TT88 PAY 1-Nhập đúng nội dung",
                        "789PAY 6 - Nhập đúng nội dung",
                        "MMOOPAY 6 - Nhập đúng nội dung",
                        "10 ~ 300,000"
                    ]

                while attempt < max_retries:
                    try:
                        payment_items = WebDriverWait(driver, 20).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'ul[class*="_2Cy5U6oRfyuKk0z8n_03sQ"] li.ng-scope')))
                        if not payment_items:
                            raise Exception("Không tìm thấy tùy chọn thanh toán.")
                        time.sleep(1)
                        selected_item = None
                        for target_text in target_texts:
                            for item in payment_items:
                                try:
                                    limit_element = item.find_element(By.CSS_SELECTOR, 'span[translate="OnlineDeposit_DepositLimitBetween"]')
                                    if limit_element and target_text in limit_element.text.strip():
                                        selected_item = item
                                        logging.info(f"Đã chọn tùy chọn với giới hạn {target_text} cho tài khoản {username}.")
                                        break
                                    else:
                                        h3_element = item.find_element(By.CSS_SELECTOR, 'h3[ng-if="payment.recommendationMemo"]')
                                        if h3_element and target_text == h3_element.text.strip():
                                            selected_item = item
                                            logging.info(f"Đã chọn tùy chọn với nội dung: {target_text} cho tài khoản {username}")
                                            break
                                except:
                                    continue
                            if selected_item:
                                break
                        if selected_item:
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", selected_item)
                            time.sleep(1)
                            WebDriverWait(driver, 10).until(EC.element_to_be_clickable(selected_item)).click()
                            logging.info(f"Đã nhấp vào tùy chọn thanh toán cho tài khoản {username}.")
                            ip = worker.get_current_ip()
                            worker.status_updated.emit(username, "Đã chọn tùy chọn thanh toán QR", ip)
                            return True
                        else:
                            raise Exception("Không tìm thấy tùy chọn phù hợp.")
                    except Exception as e:
                        logging.error(f"Lỗi chọn tùy chọn thanh toán (lần thử {attempt + 1}/{max_retries}) cho tài khoản {username}: {e}")
                        attempt += 1
                        if attempt < max_retries:
                            time.sleep(retry_delay)
                logging.error(f"Không tìm thấy tùy chọn phù hợp sau {max_retries} lần thử cho tài khoản {username}.")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Lỗi: Không chọn được tùy chọn thanh toán", ip)
                return False

            def attempt_switch_to_new_tab():
                attempt = 0
                while attempt < max_retries:
                    try:
                        time.sleep(2)
                        windows = driver.window_handles
                        if len(windows) < 2:
                            raise Exception("Tab mới không được mở.")
                        driver.switch_to.window(windows[-1])
                        logging.info(f"Đã chuyển sang tab mới cho tài khoản {username}.")
                        ip = worker.get_current_ip()
                        worker.status_updated.emit(username, "Đã chuyển sang tab mới", ip)
                        return True
                    except Exception as e:
                        logging.error(f"Lỗi chuyển tab mới (lần thử {attempt + 1}/{max_retries}) cho tài khoản {username}: {e}")
                        attempt += 1
                        if attempt < max_retries:
                            time.sleep(retry_delay)
                logging.error(f"Lỗi chuyển tab mới sau {max_retries} lần thử cho tài khoản {username}.")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Lỗi: Không chuyển được tab mới", ip)
                return False

            def attempt_select_bank():
                attempt = 0
                while attempt < max_retries:
                    try:
                        WebDriverWait(driver, 10).until(
                            lambda d: d.execute_script('return document.readyState') == 'complete'
                        )
                        logging.info(f"JavaScript đã tải hoàn tất cho tài khoản {username}.")
                        bank_list_selector = 'div.bank-list-animation a'
                        try:
                            bank_elements = WebDriverWait(driver, 5).until(
                                EC.presence_of_all_elements_located((By.CSS_SELECTOR, bank_list_selector)))
                            bank_info = []
                            for bank in bank_elements:
                                try:
                                    bank_name = bank.find_element(By.CSS_SELECTOR, 'b').text.strip()
                                    bank_info.append({'name': bank_name, 'element': bank})
                                except:
                                    continue
                            if bank_info:
                                selected_bank = random.choice(bank_info)
                                logging.info(f"Chọn ngân hàng: {selected_bank['name']} cho tài khoản {username}")
                                selected_bank['element'].click()
                                ip = worker.get_current_ip()
                                worker.status_updated.emit(username, f"Đã chọn ngân hàng: {selected_bank['name']}", ip)
                                return True
                        except Exception as e:
                            logging.info(f"Danh sách ngân hàng đầu tiên không khả dụng: {e} cho tài khoản {username}")
                        try:
                            WebDriverWait(driver, 5).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.sekectList'))
                            )
                            logging.info(f"Đã tìm thấy div.sekectList cho tài khoản {username}")
                        except Exception as e:
                            logging.error(f"Không tìm thấy div.sekectList: {e} cho tài khoản {username}")
                            attempt += 1
                            if attempt < max_retries:
                                time.sleep(retry_delay)
                            continue
                        second_bank_list_selector = 'div.sekectList div.seItem button[class*="btn-danger"]'
                        second_bank_elements = WebDriverWait(driver, 5).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, second_bank_list_selector)))
                        logging.info(f"Tìm thấy {len(second_bank_elements)} nút trong div.sekectList cho tài khoản {username}")
                        second_bank_info = []
                        for bank in second_bank_elements:
                            try:
                                parent = bank.find_element(By.XPATH, "..")
                                img_element = parent.find_element(By.CSS_SELECTOR, 'img')
                                src = img_element.get_attribute('src')
                                if not src:
                                    logging.warning(f"Thẻ img không có thuộc tính src cho tài khoản {username}")
                                    continue
                                bank_name = src.split('/')[-1].replace('.png', '') if src else 'Unknown'
                                second_bank_info.append({'name': bank_name, 'element': bank})
                                logging.info(f"Thêm ngân hàng: {bank_name} cho tài khoản {username}")
                            except Exception as e:
                                logging.warning(f"Không lấy được tên ngân hàng từ seItem: {e} cho tài khoản {username}")
                                continue
                        if second_bank_info:
                            selected_bank = random.choice(second_bank_info)
                            logging.info(f"Chọn ngân hàng thứ hai: {selected_bank['name']} cho tài khoản {username}")
                            driver.execute_script("arguments[0].scrollIntoView(true);", selected_bank['element'])
                            WebDriverWait(driver, 5).until(EC.element_to_be_clickable(selected_bank['element'])).click()
                            ip = worker.get_current_ip()
                            worker.status_updated.emit(username, f"Đã chọn ngân hàng: {selected_bank['name']} từ sekectList", ip)
                            return True
                        else:
                            logging.error(f"Danh sách second_bank_info rỗng, không tìm thấy ngân hàng hợp lệ cho tài khoản {username}.")
                            raise Exception("Không tìm thấy danh sách ngân hàng trong sekectList.")
                    except Exception as e:
                        logging.error(f"Lỗi chọn ngân hàng (lần thử {attempt + 1}/{max_retries}) cho tài khoản {username}: {e}")
                        attempt += 1
                        if attempt < max_retries:
                            time.sleep(retry_delay)
                logging.error(f"Không tìm thấy ngân hàng sau {max_retries} lần thử, bỏ qua cho tài khoản {username}.")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Lỗi: Không tìm thấy ngân hàng", ip)
                return None

            def attempt_get_transaction_info(driver, username, worker, max_retries=3, retry_delay=2):
                attempt = 0
                bank_name = account_number = account_holder = transfer_amount = transfer_content = "N/A"
                while attempt < max_retries:
                    try:
                        # Thử lấy từ QR code
                        try:
                            qr_code_content = WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, 'div#qr-code-content img.img_qr_pay')))
                            src = qr_code_content.get_attribute('src')
                            pattern = r'qr/([A-Z]+)-(\d+)-(\d+)-([A-Z0-9]+)-compact2\.jpg\?accountName=([^+]+(?:\+[^+]+)*)'
                            match = re.search(pattern, src)
                            if match:
                                bank_name_raw = match.group(1)
                                bank_name = re.sub(r'ngân\s*hàng\s*', '', bank_name_raw,
                                                   flags=re.IGNORECASE).strip() or bank_name_raw
                                if bank_name.upper() == "ICB":
                                    bank_name = "VIETINBANK"
                                account_number = match.group(2)
                                transfer_amount = match.group(3)
                                transfer_content = match.group(4)
                                account_holder = match.group(5).replace('+', ' ')
                                logging.info(
                                    f"Thông tin từ QR code cho tài khoản {username}: Ngân hàng={bank_name}, Số tài khoản={account_number}, "
                                    f"Chủ tài khoản={account_holder}, Số tiền={transfer_amount}, Nội dung={transfer_content}")
                                ip = worker.get_current_ip()
                                worker.status_updated.emit(username, "Đã lấy thông tin giao dịch từ QR", ip)
                                if account_holder == "N/A" or transfer_amount == "N/A":
                                    raise Exception("QR thiếu thông tin, thử lấy từ nguồn khác")
                                return bank_name, account_number, account_holder, transfer_amount, transfer_content
                        except Exception as e:
                            logging.info(f"Không lấy được thông tin từ QR code cho tài khoản {username}: {e}")

                        # Thử lấy từ bank-info bluebox (cũ)
                        try:
                            bank_info_div = WebDriverWait(driver, 20).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.bank-info.bluebox')))
                            logging.debug(f"Tìm thấy div.bank-info.bluebox cho tài khoản {username}")
                            logging.debug(f"HTML của div.bank-info.bluebox: {bank_info_div.get_attribute('outerHTML')}")
                            try:
                                bank_name_raw = bank_info_div.find_element(By.CSS_SELECTOR, '#bankName').text.strip()
                                bank_name = re.sub(r'ngân\s*hàng\s*', '', bank_name_raw,
                                                   flags=re.IGNORECASE).strip() or bank_name_raw
                                if bank_name.upper() == "ICB":
                                    bank_name = "VIETINBANK"
                            except Exception as e:
                                logging.debug(f"Lỗi lấy bank_name từ bank-info bluebox: {e}")
                                bank_name = "N/A"
                            try:
                                # Thử lấy #name, username, hoặc name
                                try:
                                    account_holder = bank_info_div.find_element(By.CSS_SELECTOR, '#name').text.strip()
                                except:
                                    try:
                                        account_holder = bank_info_div.find_element(By.CSS_SELECTOR,
                                                                                    '#username').text.strip()
                                    except:
                                        account_holder = bank_info_div.find_element(By.CSS_SELECTOR,
                                                                                    '#Name').text.strip()
                            except Exception as e:
                                logging.debug(f"Lỗi lấy account_holder từ bank-info bluebox: {e}")
                                account_holder = "N/A"
                            try:
                                account_number = bank_info_div.find_element(By.CSS_SELECTOR, '#account').text.strip()
                            except Exception as e:
                                logging.debug(f"Lỗi lấy account_number từ bank-info bluebox: {e}")
                                account_number = "N/A"
                            try:
                                transfer_content = bank_info_div.find_element(By.CSS_SELECTOR, '#message').text.strip()
                            except Exception as e:
                                logging.debug(f"Lỗi lấy transfer_content từ bank-info bluebox: {e}")
                                transfer_content = "N/A"
                            try:
                                # Thử lấy span#money, text-money, hoặc #money
                                try:
                                    money_element = WebDriverWait(driver, 10).until(
                                        EC.presence_of_element_located((By.CSS_SELECTOR, 'span#money')))
                                except:
                                    try:
                                        money_element = WebDriverWait(driver, 10).until(
                                            EC.presence_of_element_located((By.CSS_SELECTOR, '#text-money')))
                                    except:
                                        money_element = WebDriverWait(driver, 10).until(
                                            EC.presence_of_element_located((By.CSS_SELECTOR, '#money')))
                                transfer_amount_raw = money_element.text.strip()
                                logging.debug(f"Tìm thấy transfer_amount_raw: {transfer_amount_raw}")
                                transfer_amount = transfer_amount_raw.replace(',', '').replace(' VND', '').replace(' ',
                                                                                                                   '')
                                if not transfer_amount.replace('.', '').isdigit():
                                    raise ValueError(f"Số tiền không hợp lệ: {transfer_amount}")
                            except Exception as e:
                                logging.debug(f"Lỗi lấy transfer_amount từ span#money, text-money, hoặc #money: {e}")
                                transfer_amount = "N/A"
                            logging.info(
                                f"Thông tin từ bank-info bluebox (cũ) cho tài khoản {username}: Ngân hàng={bank_name}, Số tài khoản={account_number}, "
                                f"Chủ tài khoản={account_holder}, Số tiền={transfer_amount}, Nội dung={transfer_content}")
                            ip = worker.get_current_ip()
                            worker.status_updated.emit(username, "Đã lấy thông tin giao dịch từ bank-info bluebox (cũ)",
                                                       ip)
                            if account_holder != "N/A" and transfer_amount != "N/A":
                                return bank_name, account_number, account_holder, transfer_amount, transfer_content
                            else:
                                raise Exception(
                                    f"Bank-info bluebox (cũ) thiếu thông tin: account_holder={account_holder}, transfer_amount={transfer_amount}")
                        except Exception as e:
                            logging.info(
                                f"Không lấy được thông tin từ bank-info bluebox (cũ) cho tài khoản {username}: {e}")

                        # Thử lấy từ new bank info
                        try:
                            new_bank_info_div = WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located(
                                    (By.CSS_SELECTOR, 'div.css-1tcuxkf.col-12.col-md-8.border-right')))
                            try:
                                bank_name_raw = new_bank_info_div.find_element(By.CSS_SELECTOR,
                                                                               'span.zaui-text.zaui-text-large').text.strip()
                                bank_name = re.sub(r'ngân\s*hàng\s*', '', bank_name_raw,
                                                   flags=re.IGNORECASE).strip() or bank_name_raw
                                if bank_name.upper() == "ICB":
                                    bank_name = "VIETINBANK"
                            except:
                                bank_name = "N/A"
                            try:
                                account_holder_elements = new_bank_info_div.find_elements(By.CSS_SELECTOR,
                                                                                          'li.zaui-list-item div.zaui-list-item-content span.zaui-text')
                                account_holder = account_holder_elements[1].text.strip()
                            except:
                                account_holder = "N/A"
                            try:
                                account_number_elements = new_bank_info_div.find_elements(By.CSS_SELECTOR,
                                                                                          'li.zaui-list-item div.zaui-list-item-content span.zaui-text')
                                account_number = account_number_elements[3].text.strip()
                            except:
                                account_number = "N/A"
                            try:
                                transfer_content = new_bank_info_div.find_element(By.CSS_SELECTOR,
                                                                                  'li.zaui-list-item div.zaui-list-item-content span.zaui-text.zaui-text-large').text.strip()
                            except:
                                transfer_content = "N/A"
                            try:
                                money_elements = new_bank_info_div.find_elements(By.CSS_SELECTOR,
                                                                                 'li.zaui-list-item div.zaui-list-item-content span.zaui-text.zaui-text-large')
                                transfer_amount = money_elements[1].text.strip().replace(',', '').replace(' VND', '')
                                if not transfer_amount.isdigit():
                                    raise ValueError(f"Số tiền không hợp lệ: {transfer_amount}")
                            except:
                                transfer_amount = "N/A"
                            logging.info(
                                f"Thông tin từ new bank info cho tài khoản {username}: Ngân hàng={bank_name}, Số tài khoản={account_number}, "
                                f"Chủ tài khoản={account_holder}, Số tiền={transfer_amount}, Nội dung={transfer_content}")
                            ip = worker.get_current_ip()
                            worker.status_updated.emit(username, "Đã lấy thông tin giao dịch từ new bank info", ip)
                            if account_holder != "N/A":
                                return bank_name, account_number, account_holder, transfer_amount, transfer_content
                            else:
                                raise Exception("New bank info thiếu thông tin")
                        except Exception as e:
                            logging.info(f"Không lấy được thông tin từ new bank info cho tài khoản {username}: {e}")

                        # Thử lấy từ boxright bank-info
                        try:
                            WebDriverWait(driver, 30).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.boxright div.bank-info.bluebox')))
                            boxright_div = driver.find_element(By.CSS_SELECTOR, 'div.boxright div.bank-info.bluebox')
                            try:
                                bank_name_raw = boxright_div.find_element(By.CSS_SELECTOR, '#bankName').text.strip()
                                bank_name = re.sub(r'ngân\s*hàng\s*', '', bank_name_raw,
                                                   flags=re.IGNORECASE).strip() or bank_name_raw
                                if bank_name.upper() == "ICB":
                                    bank_name = "VIETINBANK"
                            except:
                                bank_name = "N/A"
                            try:
                                # Thử lấy #name, username, hoặc name
                                try:
                                    account_holder = boxright_div.find_element(By.CSS_SELECTOR, '#name').text.strip()
                                except:
                                    try:
                                        account_holder = boxright_div.find_element(By.CSS_SELECTOR,
                                                                                   'username').text.strip()
                                    except:
                                        account_holder = boxright_div.find_element(By.CSS_SELECTOR, 'name').text.strip()
                            except:
                                account_holder = "N/A"
                            try:
                                account_number = boxright_div.find_element(By.CSS_SELECTOR, '#account').text.strip()
                            except:
                                account_number = "N/A"
                            try:
                                transfer_content = boxright_div.find_element(By.CSS_SELECTOR, '#message').text.strip()
                            except:
                                transfer_content = "N/A"
                            try:
                                # Thử lấy span#money, text-money, hoặc #money
                                try:
                                    money_element = boxright_div.find_element(By.CSS_SELECTOR, 'span#money')
                                except:
                                    try:
                                        money_element = boxright_div.find_element(By.CSS_SELECTOR, 'text-money')
                                    except:
                                        money_element = boxright_div.find_element(By.CSS_SELECTOR, '#money')
                                transfer_amount = money_element.text.strip().replace(',', '')
                                if not transfer_amount.replace('.', '').isdigit():
                                    raise ValueError(f"Số tiền không hợp lệ: {transfer_amount}")
                            except:
                                transfer_amount = "N/A"
                            logging.info(
                                f"Thông tin từ boxright bank-info cho tài khoản {username}: Ngân hàng={bank_name}, Số tài khoản={account_number}, "
                                f"Chủ tài khoản={account_holder}, Số tiền={transfer_amount}, Nội dung={transfer_content}")
                            ip = worker.get_current_ip()
                            worker.status_updated.emit(username, "Đã lấy thông tin giao dịch từ boxright bank-info", ip)
                            if account_holder != "N/A":
                                return bank_name, account_number, account_holder, transfer_amount, transfer_content
                            else:
                                raise Exception(f"Boxright bank-info thiếu thông tin: account_holder={account_holder}")
                        except Exception as e:
                            logging.info(
                                f"Không lấy được thông tin từ boxright bank-info cho tài khoản {username}: {e}")

                        raise Exception("Không thể lấy thông tin giao dịch từ bất kỳ nguồn nào.")
                    except Exception as e:
                        logging.error(
                            f"Lỗi lấy thông tin giao dịch (lần thử {attempt + 1}/{max_retries}) cho tài khoản {username}: {e}")
                        attempt += 1
                        if attempt < max_retries:
                            time.sleep(retry_delay)
                logging.error(f"Lỗi lấy thông tin giao dịch sau {max_retries} lần thử cho tài khoản {username}.")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Lỗi: Không lấy được thông tin giao dịch", ip)
                return None

            if not attempt_select_payment_method():
                with status_lock:
                    transaction_status[username] = False
                return False
            if not attempt_select_payment_option():
                with status_lock:
                    transaction_status[username] = False
                return False
            try:
                bank_select = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'select[ng-model="$ctrl.form.bank.value"]')))
                bank_options = bank_select.find_elements(By.CSS_SELECTOR, 'option')
                if bank_options:
                    selected_bank = random.choice(bank_options)
                    selected_bank.click()
                    logging.info(f"Đã chọn ngân hàng: {selected_bank.text} cho tài khoản {username}")
                    ip = worker.get_current_ip()
                    worker.status_updated.emit(username, f"Đã chọn ngân hàng: {selected_bank.text}", ip)
                else:
                    logging.info(f"Không tìm thấy tùy chọn ngân hàng, bỏ qua cho tài khoản {username}.")
            except Exception as e:
                logging.warning(f"Lỗi khi chọn ngân hàng: {e} cho tài khoản {username}. Bỏ qua.")
            try:
                amount = int(amount)
                if 10 <= amount <= 300000:
                    amount_input = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[ng-model="$ctrl.form.amount.value"]')))
                    driver.execute_script("arguments[0].value = '';", amount_input)
                    amount_input.send_keys(str(amount))
                    logging.info(f"Đã điền số tiền: {amount} cho tài khoản {username}")
                    ip = worker.get_current_ip()
                    worker.status_updated.emit(username, f"Đã điền số tiền: {amount}", ip)
                else:
                    logging.error(f"Số tiền {amount} không nằm trong khoảng 10 ~ 300,000 cho tài khoản {username}.")
                    ip = worker.get_current_ip()
                    worker.status_updated.emit(username, f"Lỗi: Số tiền {amount} không hợp lệ", ip)
                    with status_lock:
                        transaction_status[username] = False
                    return False
            except Exception as e:
                logging.error(f"Lỗi khi điền số tiền: {e} cho tài khoản {username}")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, f"Lỗi khi điền số tiền: {str(e)}", ip)
                with status_lock:
                    transaction_status[username] = False
                return False
            try:
                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'span[translate="OnlineDeposit_Pay_Immediately"]'))).click()
                logging.info(f"Đã nhấp nút nạp tiền cho tài khoản {username}.")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Đã gửi yêu cầu nạp tiền", ip)
            except Exception as e:
                logging.error(f"Lỗi khi nhấp nút nạp tiền: {e} cho tài khoản {username}")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, f"Lỗi khi gửi yêu cầu nạp tiền: {str(e)}", ip)
                with status_lock:
                    transaction_status[username] = False
                return False
            if not attempt_switch_to_new_tab():
                with status_lock:
                    transaction_status[username] = False
                return False
            attempt_select_bank()
            transaction_info = attempt_get_transaction_info(driver, username, worker)
            if not transaction_info:
                with status_lock:
                    transaction_status[username] = False
                return False
            bank_name, account_number, account_holder, transfer_amount, transfer_content = transaction_info
            csv_data = write_to_csv(account_number, account_holder, transfer_amount, bank_name, transfer_content, "", time.strftime("%Y-%m-%d %H:%M:%S"), phone_number, overwrite)
            txt_data = f"{username}|{transfer_content}|{transfer_amount}|{bank_name}|{time.strftime('%Y-%m-%d %H:%M:%S')}|Thành công"

            if api_mode != "Nạp Tay":
                with csv_lock:
                    csv_rows = []
                    try:
                        with open('nap_tien.csv', 'r', newline='', encoding='utf-8') as f:
                            reader = csv.DictReader(f)
                            csv_rows = list(reader)
                        logging.info(f"Đã đọc {len(csv_rows)} dòng từ nap_tien.csv cho tài khoản {username}")
                    except Exception as e:
                        logging.error(f"Lỗi khi đọc nap_tien.csv: {e} cho tài khoản {username}")
                    for row in csv_rows:
                        row_account_number = row['SO TAI KHOAN'].replace('=', '').replace('"', '').strip()
                        if (row_account_number == account_number and
                                row['NGAN HANG'].strip() == bank_name and
                                row['NOI DUNG'].strip() == transfer_content and
                                row['TRANG THAI'].strip() == ""):
                            try:
                                so_tai_khoan = row_account_number
                                ngan_hang = row['NGAN HANG'].strip()
                                so_tien = int(row['SO TIEN'].strip())
                                noi_dung = row['NOI DUNG'].strip()
                                phone = row['SDT'].replace('=', '').replace('"', '').strip()
                                if not phone or not phone.isdigit():
                                    logging.warning(f"Số điện thoại không hợp lệ cho dòng {so_tai_khoan}: {phone} cho tài khoản {username}.")
                                    ip = worker.get_current_ip()
                                    worker.status_updated.emit(username, f"Số điện thoại không hợp lệ: {phone}", ip)
                                    continue
                                phone = int(phone)
                                for attempt in range(max_api_retries):
                                    is_success, message = transfer_money_api(
                                        recvCustBankAcc=so_tai_khoan,
                                        recvBankcode=ngan_hang,
                                        transAmount=so_tien,
                                        transContent=noi_dung,
                                        phone=phone,
                                        api_mode=api_mode
                                    )
                                    with response_lock:
                                        response_data = f"{username}|{time.strftime('%Y-%m-%d %H:%M:%S')}|{is_success}|{message}"
                                        with open('response.txt', 'a', encoding='utf-8') as f:
                                            f.write(response_data + "\n")
                                        logging.info(f"Đã ghi API response vào response.txt: {response_data} cho tài khoản {username}")
                                    ip = worker.get_current_ip()
                                    api_status = f"Gọi API lần {attempt + 1}/{max_api_retries} thành công" if is_success and message == "payment success" else f"Gọi API lần {attempt + 1}/{max_api_retries} thất bại: {message}"
                                    worker.status_updated.emit(username, api_status, ip)
                                    if is_success and message == "payment success":
                                        break
                                    elif message == "Payment fail" and attempt < max_api_retries - 1:
                                        logging.info(f"API thất bại với 'Payment fail', thử lại sau {api_retry_delay} giây (lần {attempt + 2}/{max_api_retries}) cho tài khoản {username}")
                                        time.sleep(api_retry_delay)
                                        continue
                                    else:
                                        break
                                if is_success and message == "payment success":
                                    row['TRANG THAI'] = "Thành công"
                                    updated_rows = []
                                    for r in csv_rows:
                                        if (r['SO TAI KHOAN'] == row['SO TAI KHOAN'] and
                                                r['NGAN HANG'] == row['NGAN HANG'] and
                                                r['NOI DUNG'] == row['NOI DUNG'] and
                                                r['THOI GIAN'] == row['THOI GIAN']):
                                            r['TRANG THAI'] = "Thành công"
                                        updated_rows.append(r)
                                    with open('nap_tien.csv', 'w', newline='', encoding='utf-8') as f:
                                        writer = csv.DictWriter(f, fieldnames=['SO TAI KHOAN', 'CHU TAI KHOAN', 'SO TIEN', 'NGAN HANG', 'NOI DUNG', 'TRANG THAI', 'THOI GIAN', 'SDT'])
                                        writer.writeheader()
                                        writer.writerows(updated_rows)
                                    logging.info(f"Đã cập nhật nap_tien.csv với trạng thái 'Thành công' cho tài khoản {username}.")
                                    with open(txt_filename, 'a', encoding='utf-8') as f:
                                        f.write(txt_data + "\n")
                                    logging.info(f"Đã ghi thông tin vào {txt_filename}: {txt_data} cho tài khoản {username}")
                                else:
                                    logging.error(f"Gọi API thất bại cho {so_tai_khoan} sau {max_api_retries} lần thử: {message} cho tài khoản {username}")
                            except Exception as e:
                                with response_lock:
                                    response_data = f"{username}|{time.strftime('%Y-%m-%d %H:%M:%S')}|False|Lỗi khi gọi API: {str(e)}"
                                    with open('response.txt', 'a', encoding='utf-8') as f:
                                        f.write(response_data + "\n")
                                    logging.info(f"Đã ghi lỗi API vào response.txt: {response_data} cho tài khoản {username}")
                                ip = worker.get_current_ip()
                                logging.error(f"Lỗi khi gọi API cho dòng {so_tai_khoan}: {e} cho tài khoản {username}")
                                worker.status_updated.emit(username, f"Gọi API thất bại: {str(e)}", ip)
            else:
                logging.info(f"Chế độ Nạp Tay: Không gọi API cho tài khoản {username}")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Chờ nạp tay", ip)

            ip = worker.get_current_ip()
            worker.status_updated.emit(username, "Đang chờ trạng thái giao dịch", ip)

            try:
                max_wait_time = 600 if api_mode == "Nạp Tay" else 300
                polling_interval = 5
                start_time = time.time()

                success_selectors = [
                    ('div.success div.titles', "GIAO DỊCH THÀNH CÔNG"),
                    ('div.success div.title-tip', "Giao dịch thành công"),
                    ('div.notice.successbox div.tips', "Giao dịch thành công"),
                    ('div.user-info.border h4.text-success.fw-bold', "Giao dịch thành công"),
                    ('h2[style*="color: #b5b5b5"][style*="text-align: center"]', "Thanh toán đơn hàng thành công"),
                ]

                while time.time() - start_time < max_wait_time:
                    for selector, expected_text in success_selectors:
                        try:
                            success_element = driver.find_element(By.CSS_SELECTOR, selector)
                            if success_element.text.strip().lower() == expected_text.lower():
                                logging.info(
                                    f"Xác nhận: Giao dịch thành công (từ {selector}) cho tài khoản {username}.")
                                csv_data = write_to_csv(
                                    "", "", "", "", "", "Thành công", time.strftime("%Y-%m-%d %H:%M:%S"),
                                    phone_number, overwrite
                                )
                                txt_data = f"{username}||||{time.strftime('%Y-%m-%d %H:%M:%S')}|Thành công"
                                with open(txt_filename, 'a', encoding='utf-8') as f:
                                    f.write(txt_data + "\n")
                                logging.info(
                                    f"Đã ghi thông tin vào {txt_filename}: {txt_data} cho tài khoản {username}")
                                driver.quit()
                                ip = worker.get_current_ip()
                                worker.status_updated.emit(username, "Giao dịch thành công", ip)
                                with status_lock:
                                    transaction_status[username] = True
                                return True
                        except Exception:
                            pass
                    time.sleep(polling_interval)

                logging.error(
                    f"Hết thời gian chờ, không tìm thấy thông báo giao dịch thành công cho tài khoản {username}.")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Lỗi: Hết thời gian chờ giao dịch", ip)
                with status_lock:
                    transaction_status[username] = False
                return False
            except Exception as e:
                logging.error(f"Lỗi khi kiểm tra trạng thái giao dịch: {e} cho tài khoản {username}")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, f"Lỗi: {str(e)}", ip)
                with status_lock:
                    transaction_status[username] = False
                return False

        elif mode == "Momo":
            logging.info(f"Chế độ: Momo cho tài khoản {username}")
            def attempt_select_momo_payment():
                attempt = 0
                selector = 'li[ng-repeat="item in $ctrl.payments"] span[translate="OnlineDeposit_MomoPay"]'
                while attempt < max_retries:
                    try:
                        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector))).click()
                        ip = worker.get_current_ip()
                        worker.status_updated.emit(username, "Đã chọn Momo Pay", ip)
                        logging.info(f"Đã chọn phương thức Momo Pay cho tài khoản {username}.")
                        return True
                    except Exception:
                        payment_items = WebDriverWait(driver, 10).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'li[ng-repeat="item in $ctrl.payments"]')))
                        target_element = None
                        for item in payment_items:
                            try:
                                span = item.find_element(By.CSS_SELECTOR, 'span[translate="OnlineDeposit_MomoPay"]')
                                if span:
                                    target_element = item
                                    break
                            except:
                                continue
                        if target_element:
                            driver.execute_script("arguments[0].scrollIntoView(true);", target_element)
                            WebDriverWait(driver, 10).until(EC.element_to_be_clickable(target_element)).click()
                            ip = worker.get_current_ip()
                            worker.status_updated.emit(username, "Đã chọn Momo Pay", ip)
                            logging.info(f"Đã chọn phương thức Momo Pay (kiểm tra danh sách) cho tài khoản {username}.")
                            return True
                        attempt += 1
                        if attempt < max_retries:
                            time.sleep(retry_delay)
                    except Exception as e:
                        logging.error(f"Lỗi chọn phương thức Momo Pay (lần thử {attempt + 1}/{max_retries}) cho tài khoản {username}: {e}")
                        attempt += 1
                        if attempt < max_retries:
                            time.sleep(retry_delay)
                logging.error(f"Không tìm thấy phương thức Momo Pay sau {max_retries} lần thử cho tài khoản {username}.")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Lỗi: Không chọn được Momo Pay", ip)
                return False

            def attempt_select_payment_option():
                attempt = 0
                # Đọc target_texts từ file congnap.txt
                target_texts = []
                try:
                    with open(CONGNAP_FILE, 'r', encoding='utf-8') as f:
                        target_texts = [line.strip() for line in f if line.strip()]
                    logging.info(f"Đã đọc {len(target_texts)} mục từ congnap.txt cho tài khoản {username}")
                except FileNotFoundError:
                    logging.error(
                        f"Không tìm thấy file congnap.txt, sử dụng danh sách mặc định cho tài khoản {username}")
                    target_texts = [
                        "10 ~ 15,000",
                        "20 ~ 300,000",
                    ]
                except Exception as e:
                    logging.error(f"Lỗi khi đọc congnap.txt: {e} cho tài khoản {username}")
                    target_texts = [
                        "10 ~ 15,000",
                        "20 ~ 300,000",
                    ]

                while attempt < max_retries:
                    try:
                        payment_items = WebDriverWait(driver, 20).until(
                            EC.presence_of_all_elements_located(
                                (By.CSS_SELECTOR, 'ul[class*="_2Cy5U6oRfyuKk0z8n_03sQ"] li.ng-scope')))
                        if not payment_items:
                            raise Exception("Không tìm thấy tùy chọn thanh toán.")
                        time.sleep(1)
                        selected_item = None
                        for target_text in target_texts:
                            for item in payment_items:
                                try:
                                    limit_element = item.find_element(By.CSS_SELECTOR,
                                                                      'span[translate="OnlineDeposit_DepositLimitBetween"]')
                                    if limit_element and target_text in limit_element.text.strip():
                                        selected_item = item
                                        logging.info(
                                            f"Đã chọn tùy chọn với giới hạn {target_text} cho tài khoản {username}.")
                                        break
                                    else:
                                        h3_element = item.find_element(By.CSS_SELECTOR,
                                                                       'h3[ng-if="payment.recommendationMemo"]')
                                        if h3_element and target_text == h3_element.text.strip():
                                            selected_item = item
                                            logging.info(
                                                f"Đã chọn tùy chọn với nội dung: {target_text} cho tài khoản {username}")
                                            break
                                except:
                                    continue
                            if selected_item:
                                break
                        if selected_item:
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});",
                                                  selected_item)
                            time.sleep(1)
                            WebDriverWait(driver, 10).until(EC.element_to_be_clickable(selected_item)).click()
                            logging.info(f"Đã nhấp vào tùy chọn thanh toán Momo cho tài khoản {username}.")
                            ip = worker.get_current_ip()
                            worker.status_updated.emit(username, f"Đã chọn tùy chọn thanh toán Momo: {target_text}", ip)
                            return True
                        else:
                            raise Exception("Không tìm thấy tùy chọn phù hợp.")
                    except Exception as e:
                        logging.error(
                            f"Lỗi chọn tùy chọn thanh toán Momo (lần thử {attempt + 1}/{max_retries}) cho tài khoản {username}: {e}")
                        attempt += 1
                        if attempt < max_retries:
                            time.sleep(retry_delay)
                logging.error(f"Không tìm thấy tùy chọn phù hợp sau {max_retries} lần thử cho tài khoản {username}.")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Lỗi: Không chọn được tùy chọn thanh toán Momo", ip)
                return False

            if not attempt_select_momo_payment():
                with status_lock:
                    transaction_status[username] = False
                return False
            if not attempt_select_payment_option():
                with status_lock:
                    transaction_status[username] = False
                return False
            try:
                amount = int(amount)
                if 10 <= amount <= 15000:
                    amount_input = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[ng-model="$ctrl.form.amount.value"]')))
                    driver.execute_script("arguments[0].value = '';", amount_input)
                    amount_input.send_keys(str(amount))
                    logging.info(f"Đã điền số tiền: {amount} cho tài khoản {username}")
                    ip = worker.get_current_ip()
                    worker.status_updated.emit(username, f"Đã điền số tiền: {amount}", ip)
                else:
                    logging.error(f"Số tiền {amount} không nằm trong khoảng 10 ~ 15,000 cho tài khoản {username}.")
                    ip = worker.get_current_ip()
                    worker.status_updated.emit(username, f"Lỗi: Số tiền {amount} không hợp lệ", ip)
                    with status_lock:
                        transaction_status[username] = False
                    return False
            except Exception as e:
                logging.error(f"Lỗi khi điền số tiền: {e} cho tài khoản {username}")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, f"Lỗi khi điền số tiền: {str(e)}", ip)
                with status_lock:
                    transaction_status[username] = False
                return False
            try:
                pay_button = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'span[translate="OnlineDeposit_Pay_Immediately"]')))
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", pay_button)
                time.sleep(1)  # Đợi thêm để đảm bảo nút sẵn sàng
                driver.execute_script("arguments[0].click();", pay_button)
                logging.info(f"Đã nhấp nút thanh toán Momo cho tài khoản {username}.")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Đã gửi yêu cầu thanh toán Momo", ip)
            except Exception as e:
                logging.error(f"Lỗi khi nhấp nút thanh toán Momo: {e} cho tài khoản {username}")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, f"Lỗi khi gửi yêu cầu thanh toán Momo: {str(e)}", ip)
                with status_lock:
                    transaction_status[username] = False
                return False

            # Đảm bảo switch sang tab mới
            try:
                time.sleep(5)  # Chờ tab mới load
                if len(driver.window_handles) > 1:
                    driver.switch_to.window(driver.window_handles[-1])
                    logging.info(f"Đã chuyển sang tab mới cho tài khoản {username}.")
                    ip = worker.get_current_ip()
                    worker.status_updated.emit(username, "Đã chuyển sang tab mới", ip)
                else:
                    logging.warning(f"Không có tab mới được mở cho tài khoản {username}.")
                    ip = worker.get_current_ip()
                    worker.status_updated.emit(username, "Cảnh báo: Không có tab mới", ip)
            except Exception as e:
                logging.error(f"Lỗi khi chuyển tab mới: {e} cho tài khoản {username}")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, f"Lỗi khi chuyển tab: {str(e)}", ip)
                with status_lock:
                    transaction_status[username] = False
                return False


            ip = worker.get_current_ip()
            worker.status_updated.emit(username, "Đang chờ trạng thái giao dịch", ip)

            try:
                max_wait_time = 300 if api_mode == "Nạp Tay" else 300
                polling_interval = 5
                start_time = time.time()

                success_selectors = [
                    ('div.success-message h2', "GIAO DỊCH THÀNH CÔNG"),
                    ('div.success h2.titles', "GIAO DỊCH THÀNH CÔNG"),
                    ('h2.success-title', "GIAO DỊCH THÀNH CÔNG"),
                    ('div.alert-success', "GIAO DỊCH THÀNH CÔNG"),
                    ('p.success-text', "GIAO DỊCH THÀNH CÔNG"),
                    ('div#success-modal h2', "GIAO DỊCH THÀNH CÔNG"),
                    ('p.titles', "Giao dịch thành công"),
                    ('div#transaction_success h2.label-text', "GIAO DỊCH THÀNH CÔNG"),
                    ('div.success div.titles', "GIAO DỊCH THÀNH CÔNG"),
                    ('div.success div.title-tip', "Giao dịch thành công"),
                    ('div.notice.successbox div.tips', "Giao dịch thành công"),
                    ('div.user-info.border h4.text-success.fw-bold', "Giao dịch thành công"),
                    ('h2[style*="color: #b5b5b5"][style*="text-align: center"]', "Thanh toán đơn hàng thành công"),
                    ('div.titles', "Thành công"),  # Thêm: Bộ chọn cho div.titles với text "Thành công"
                    ('div.success div.titles', "Thành công"),
                    # Thêm: Bộ chọn cho div.titles trong div.success với text "Thành công"
                    ('div.success span', "Giao dịch thành công")
                    # Thêm: Bộ chọn cho span trong div.success với text "Giao dịch thành công"
                ]

                while time.time() - start_time < max_wait_time:
                    for selector, expected_text in success_selectors:
                        try:
                            success_element = driver.find_element(By.CSS_SELECTOR, selector)
                            if success_element.text.strip().lower() == expected_text.lower():
                                logging.info(
                                    f"Xác nhận: Giao dịch thành công (từ {selector}) cho tài khoản {username}.")
                                txt_data = f"{username}||||{time.strftime('%Y-%m-%d %H:%M:%S')}|Thành công"
                                with open(txt_filename, 'a', encoding='utf-8') as f:
                                    f.write(txt_data + "\n")
                                logging.info(
                                    f"Đã ghi thông tin vào {txt_filename}: {txt_data} cho tài khoản {username}")
                                driver.quit()
                                ip = worker.get_current_ip()
                                worker.status_updated.emit(username, "Giao dịch thành công", ip)
                                with status_lock:
                                    transaction_status[username] = True
                                return True
                        except Exception:
                            pass
                    time.sleep(polling_interval)

                logging.error(
                    f"Hết thời gian chờ, không tìm thấy thông báo giao dịch thành công cho tài khoản {username}.")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Lỗi: Hết thời gian chờ giao dịch", ip)
                with status_lock:
                    transaction_status[username] = False
                return False
            except Exception as e:
                logging.error(f"Lỗi khi kiểm tra trạng thái giao dịch: {e} cho tài khoản {username}")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, f"Lỗi: {str(e)}", ip)
                with status_lock:
                    transaction_status[username] = False
                return False

        elif mode == "CK nhanh":
            logging.info(f"Chế độ: CK nhanh cho tài khoản {username}")
            try:
                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'li[ng-repeat="item in $ctrl.payments"] span[translate="OnlineDeposit_OnlineBank"]'))).click()
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Đã chọn CK nhanh", ip)
                logging.info(f"Đã chọn phương thức CK nhanh cho tài khoản {username}.")
                bank_select = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'select[ng-model="$ctrl.form.bank.value"]')))
                bank_options = bank_select.find_elements(By.CSS_SELECTOR, 'option[ng-repeat]')
                if bank_options:
                    selected_bank = random.choice(bank_options)
                    selected_bank.click()
                    logging.info(f"Đã chọn ngân hàng: {selected_bank.text} cho tài khoản {username}")
                    ip = worker.get_current_ip()
                    worker.status_updated.emit(username, f"Đã chọn ngân hàng: {selected_bank.text}", ip)
                else:
                    logging.error(f"Không tìm thấy tùy chọn ngân hàng cho tài khoản {username}.")
                    ip = worker.get_current_ip()
                    worker.status_updated.emit(username, "Lỗi: Không tìm thấy ngân hàng", ip)
                    with status_lock:
                        transaction_status[username] = False
                    return False
                amount_input = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'input[ng-model="$ctrl.form.amount.value"]')))
                amount_input.clear()
                amount_input.send_keys(amount)
                logging.info(f"Đã điền số tiền: {amount} cho tài khoản {username}")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, f"Đã điền số tiền: {amount}", ip)
                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'span[translate="OnlineDeposit_Pay_Immediately"]'))).click()
                logging.info(f"Đã nhấp nút nạp tiền cho tài khoản {username}.")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Đã gửi yêu cầu nạp tiền CK nhanh", ip)
            except Exception as e:
                logging.error(f"Lỗi khi xử lý CK nhanh: {e} cho tài khoản {username}")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, f"Lỗi CK nhanh: {str(e)}", ip)
                with status_lock:
                    transaction_status[username] = False
                return False

        if mode not in ["Không nạp", "QR", "Momo"]:
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.success-message')))
                logging.info(f"Đã thấy thông báo thành công cho tài khoản {username}.")
                transaction_status_val = "Thành công"
                transaction_time = time.strftime("%Y-%m-%d %H:%M:%S")
                csv_data = write_to_csv("", "", "", "", "", transaction_status_val, transaction_time, phone_number, overwrite)
                txt_data = f"{username}||||{transaction_time}|{transaction_status_val}"
                with open(txt_filename, 'a', encoding='utf-8') as f:
                    f.write(txt_data + "\n")
                logging.info(f"Đã ghi thông tin vào {txt_filename}: {txt_data} cho tài khoản {username}")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Giao dịch thành công", ip)
                with status_lock:
                    transaction_status[username] = True
                return True
            except Exception:
                logging.error(f"Không thấy thông báo thành công cho tài khoản {username}.")
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Lỗi: Không thấy thông báo thành công", ip)
                with status_lock:
                    transaction_status[username] = False
                return False
        return True
    except Exception as e:
        logging.error(f"Lỗi trong quá trình xử lý JDB_BCB cho tài khoản {username}: {e}")
        ip = worker.get_current_ip()
        worker.status_updated.emit(username, f"Lỗi: {str(e)}", ip)
        with status_lock:
            transaction_status[username] = False
        return False

def move_window(window, x_pos, y_pos, browser_width, browser_height, username, max_attempts=5):
    for attempt in range(max_attempts):
        try:
            window.moveTo(x_pos, y_pos)
            window.resizeTo(browser_width, browser_height)
            time.sleep(0.5)
            if window.left == x_pos and window.top == y_pos:
                return True
            time.sleep(1)
        except Exception as e:
            logging.error(f"Lỗi di chuyển cửa sổ cho {username}: {e}")
    logging.error(f"Không thể đặt vị trí cửa sổ cho {username} sau {max_attempts} lần thử")
    return False

def login_with_selenium(link, username, password, proxy, user_agent, proxy_handler, mode, amount, phone_number, browser_width, browser_height, x_pos, y_pos, worker, api_mode):
    global status_lock, transaction_status, browser_status
    driver = None
    try:
        chrome_options = Options()
        chrome_options.add_argument(f"--user-agent={user_agent}")
        chrome_options.add_argument(f"--proxy-server={proxy}")
        chrome_options.add_argument(f"--window-size={browser_width},{browser_height}")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--mute-audio")
        chrome_options.add_argument("--disable-geolocation")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        prefs = {
            "profile.default_content_setting_values.geolocation": 2,
            "profile.default_content_setting_values.media_stream": 2,
            "profile.default_content_setting_values.media": 2,
            "profile.managed_default_content_settings.media": 2
        }
        chrome_options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_script(f"document.title = 'Chrome_{username}'")
        with file_lock:
            active_browsers.append(driver)
        for _ in range(10):
            windows = pyautogui.getWindowsWithTitle(f"Chrome_{username}")
            if windows and move_window(windows[0], x_pos, y_pos, browser_width, browser_height, username):
                break
            time.sleep(2)
        ip = worker.get_current_ip()
        worker.status_updated.emit(username, "Đang tải trang đăng nhập", ip)
        driver.get(link)
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'input[ng-model="$ctrl.user.account.value"]')))
        with status_lock:
            browser_status[username] = True
        try:
            driver.find_element(By.CSS_SELECTOR, 'input[ng-model="$ctrl.user.account.value"]').send_keys(username)
            driver.find_element(By.CSS_SELECTOR, 'input[ng-model="$ctrl.user.password.value"]').send_keys(password)
            ip = worker.get_current_ip()
            worker.status_updated.emit(username, "Đang đăng nhập", ip)
        except Exception as e:
            ip = worker.get_current_ip()
            logging.error(f"Lỗi khi nhập thông tin đăng nhập cho {username}: {str(e)}")
            worker.status_updated.emit(username, f"Lỗi nhập thông tin: {str(e)}", ip)
            return None
        max_attempts = 10
        attempt = 0
        login_success = False
        while attempt < max_attempts and not login_success:
            attempt += 1
            try:
                captcha_input = driver.find_element(By.CSS_SELECTOR, 'input[ng-model="$ctrl.code"]')
                captcha_input.clear()
            except:
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, f"Không tìm thấy ô captcha lần {attempt}", ip)
                continue
            captcha_code = handle_captcha(driver)
            if not captcha_code:
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, f"Lỗi captcha lần {attempt}", ip)
                continue
            driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div[bind-html-compile="$ctrl.content"]')))
                confirm_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.btn.btn-primary.ng-scope')))
                driver.execute_script("arguments[0].click();", confirm_button)
                time.sleep(0.5)
                continue
            except:
                login_success = True
        if login_success:
            ip = worker.get_current_ip()
            worker.status_updated.emit(username, "Đăng nhập thành công", ip)
            success = JDB_BCB(driver, link, username, password, mode, amount, phone_number, worker, api_mode)  # Truyền api_mode
            with status_lock:
                transaction_status[username] = success
                ip = worker.get_current_ip()
                worker.status_updated.emit(username, "Giao dịch " + ("thành công" if success else "thất bại"), ip)
            return driver
        else:
            ip = worker.get_current_ip()
            logging.error(f"Đăng nhập thất bại cho {username} sau {max_attempts} lần thử.")
            with status_lock:
                transaction_status[username] = False
                worker.status_updated.emit(username, "Đăng nhập thất bại", ip)
            return None
    except Exception as e:
        ip = worker.get_current_ip()
        logging.error(f"Lỗi trong login_with_selenium cho {username}: {e}")
        with status_lock:
            browser_status[username] = False
            transaction_status[username] = False
            worker.status_updated.emit(username, f"Lỗi: {str(e)}", ip)
        if driver:
            try:
                driver.quit()
                with file_lock:
                    if driver in active_browsers:
                        active_browsers.remove(driver)
                    browser_status.pop(username, None)
            except Exception as e:
                ip = worker.get_current_ip()
                logging.error(f"Lỗi đóng trình duyệt trong login_with_selenium cho {username}: {e}")
                worker.status_updated.emit(username, f"Lỗi đóng trình duyệt: {str(e)}", ip)
        return None

class Worker(QThread):
    status_updated = pyqtSignal(str, str, str)  # Thêm tham số IP vào tín hiệu

    def __init__(self, link, username, password, proxy, api_key, mode, amount, phone_number, api_mode, browser_width,
                 browser_height, x_pos, y_pos):
        super().__init__()
        self.link = link
        self.username = username
        self.password = password
        self.proxy = proxy
        self.api_key = api_key
        self.mode = mode
        self.amount = amount
        self.phone_number = phone_number
        self.api_mode = api_mode
        self.browser_width = browser_width
        self.browser_height = browser_height
        self.x_pos = x_pos
        self.y_pos = y_pos
        self.driver = None

    def get_current_ip(self):
        try:
            response = requests.get('http://ipinfo.io/ip', proxies={"http": self.proxy, "https": self.proxy}, timeout=10)
            if response.status_code == 200:
                return response.text.strip()
            else:
                return "Không lấy được IP"
        except Exception as e:
            logging.error(f"Lỗi khi lấy IP: {e}")
            return "Lỗi lấy IP"

    def run(self):
        try:
            user_agent = generate_random_user_agent()
            proxy_handler = ProxyHandler(self.api_key)
            ip = self.get_current_ip()  # Lấy IP hiện tại
            self.status_updated.emit(self.username, "Đang khởi tạo trình duyệt", ip)
            self.driver = login_with_selenium(
                self.link, self.username, self.password, self.proxy, user_agent,
                proxy_handler, self.mode, self.amount, self.phone_number,
                self.browser_width, self.browser_height, self.x_pos, self.y_pos,
                self,self.api_mode
            )
            logging.info(f"Thread cho tài khoản {self.username} hoàn tất.")
        except Exception as e:
            ip = self.get_current_ip()
            logging.error(f"Lỗi trong Worker.run cho {self.username}: {str(e)}")
            self.status_updated.emit(self.username, f"Lỗi: {str(e)}", ip)
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                    with file_lock:
                        if self.driver in active_browsers:
                            active_browsers.remove(self.driver)
                        browser_status.pop(self.username, None)
                    logging.info(f"Đã đóng trình duyệt cho {self.username}")
                except Exception as e:
                    ip = self.get_current_ip()
                    logging.error(f"Lỗi đóng trình duyệt cho {self.username}: {e}")
                    self.status_updated.emit(self.username, f"Lỗi đóng trình duyệt: {str(e)}", ip)

    def stop(self):
        if self.driver:
            try:
                self.driver.quit()
                with file_lock:
                    if self.driver in active_browsers:
                        active_browsers.remove(self.driver)
                    browser_status.pop(self.username, None)
                ip = self.get_current_ip()
                logging.info(f"Đã dừng và đóng trình duyệt cho {self.username}")
                self.status_updated.emit(self.username, "Đã dừng trình duyệt", ip)
            except Exception as e:
                ip = self.get_current_ip()
                logging.error(f"Lỗi khi dừng trình duyệt cho {self.username}: {e}")
                self.status_updated.emit(self.username, f"Lỗi dừng trình duyệt: {str(e)}", ip)

from PyQt5.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("AUTO NẠP TIỀN")
        self.setGeometry(100, 100, 600, 600)
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)
        self.table_lock = threading.Lock()

        # Link
        link_layout = QHBoxLayout()
        link_label = QLabel("Link:")
        self.link_input = QLineEdit()
        link_layout.addWidget(link_label)
        link_layout.addWidget(self.link_input)
        layout.addLayout(link_layout)

        # Chế độ, số điện thoại, số lượng web, API
        control_layout = QHBoxLayout()
        mode_label = QLabel("Chế độ:")
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["Không nạp", "VTP", "QR", "CK nhanh", "Momo"])
        control_layout.addWidget(mode_label)
        control_layout.addWidget(self.mode_combo)
        phone_label = QLabel("Số điện thoại:")
        self.phone_input = QLineEdit()
        self.phone_input.setFixedWidth(120)
        control_layout.addWidget(phone_label)
        control_layout.addWidget(self.phone_input)
        maxweb_label = QLabel("SL Web:")
        self.maxweb_input = QLineEdit()
        self.maxweb_input.setText("5")
        self.maxweb_input.setFixedWidth(30)
        control_layout.addWidget(maxweb_label)
        control_layout.addWidget(self.maxweb_input)
        api_label = QLabel("API:")
        self.api_combo = QComboBox()
        self.api_combo.addItems(["SMS", "SMOTP", "Nạp Tay"])
        self.api_combo.setFixedWidth(80)
        control_layout.addWidget(api_label)
        control_layout.addWidget(self.api_combo)
        layout.addLayout(control_layout)

        # Proxy
        proxy_label = QLabel("Proxy (api_key|proxy, mỗi dòng một proxy):")
        self.proxy_input = QTextEdit()
        layout.addWidget(proxy_label)
        layout.addWidget(self.proxy_input)

        # Tài khoản
        account_label = QLabel("Tài khoản (taikhoan|matkhau|sotien, mỗi dòng một tài khoản):")
        self.account_input = QTextEdit()
        layout.addWidget(account_label)
        layout.addWidget(self.account_input)

        # Bảng trạng thái
        self.status_table = QTableWidget()
        self.status_table.setRowCount(0)
        self.status_table.setColumnCount(4)  # Thêm cột IP, tổng cộng 4 cột
        self.status_table.setHorizontalHeaderLabels(["Tài khoản", "Trạng thái", "IP", "Thời gian"])
        self.status_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.status_table)

        # Nút điều khiển
        button_layout = QHBoxLayout()
        self.run_button = QPushButton("Chạy")
        self.run_button.clicked.connect(self.start_automation)
        button_layout.addWidget(self.run_button)
        self.close_button = QPushButton("Đóng")
        self.close_button.clicked.connect(self.close_all_browsers)
        button_layout.addWidget(self.close_button)
        self.clear_button = QPushButton("Xóa dữ liệu")  # Thêm nút Xóa dữ liệu
        self.clear_button.clicked.connect(self.clear_data_files)
        button_layout.addWidget(self.clear_button)
        layout.addLayout(button_layout)

        self.workers = []
        self.proxy_threads = []
        self.load_config()
        self.workers = []
        self.proxy_threads = []
        self.load_config()

        # GỌI HÀM KIỂM TRA CẬP NHẬT KHI KHỞI ĐỘNG
        self.check_for_updates()


    # Đặt hàm này bên trong class MainWindow
    def check_for_updates(self):
        # THAY THẾ 'TEN_DANG_NHAP' VÀ 'TEN_KHO_CHUA' CỦA BẠN
        repo_owner = "blackshark9992"
        repo_name = "NAP_TIEN_L1"

        api_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/releases/latest"

        try:
            response = requests.get(api_url, timeout=5)
            response.raise_for_status()  # Báo lỗi nếu request không thành công

            latest_release = response.json()
            latest_version = latest_release["tag_name"].replace('v', '')  # Lấy phiên bản, loại bỏ chữ 'v'

            # So sánh phiên bản
            if latest_version > CURRENT_VERSION:
                msg_box = QMessageBox()
                msg_box.setIcon(QMessageBox.Information)
                msg_box.setText(
                    f"Đã có phiên bản mới ({latest_version})!\nPhiên bản của bạn là ({CURRENT_VERSION}).\n\nBạn có muốn tải về bản cập nhật không?")
                msg_box.setWindowTitle("Thông báo cập nhật")
                msg_box.setStandardButtons(QMessageBox.Yes | QMessageBox.No)

                # Lấy link tải file .exe
                download_url = latest_release["assets"][0]["browser_download_url"]

                return_value = msg_box.exec()
                if return_value == QMessageBox.Yes:
                    webbrowser.open(download_url)  # Mở trình duyệt để tải file

        except requests.exceptions.RequestException as e:
            print(f"Lỗi khi kiểm tra cập nhật: {e}")
        except (KeyError, IndexError):
            print("Lỗi: Không tìm thấy file đính kèm trong bản phát hành mới nhất.")

    def clear_data_files(self):
        """Xóa nội dung các file nap.txt, failed_accounts.txt và nap_tien.csv"""
        try:
            files = ['nap.txt', 'failed_accounts.txt', 'nap_tien.csv','roulette.log']
            for file_name in files:
                if os.path.exists(file_name):
                    with open(file_name, 'w', encoding='utf-8') as f:
                        if file_name == 'nap_tien.csv':
                            # Ghi lại header cho file CSV
                            f.write('SO TAI KHOAN,CHU TAI KHOAN,SO TIEN,NGAN HANG,NOI DUNG,TRANG THAI,THOI GIAN,SDT\n')
                        else:
                            f.write('')  # Xóa nội dung file TXT
                    logging.info(f"Đã xóa nội dung file {file_name}")
                else:
                    logging.info(f"File {file_name} không tồn tại, bỏ qua")
            QMessageBox.information(self, "Thành công", "Đã xóa dữ liệu trong các file nap.txt, failed_accounts.txt và nap_tien.csv!")
        except Exception as e:
            logging.error(f"Lỗi khi xóa dữ liệu file: {e}")
            QMessageBox.critical(self, "Lỗi", f"Đã xảy ra lỗi khi xóa dữ liệu: {str(e)}")

    # Các phương thức khác của MainWindow giữ nguyên
    def add_status_to_table(self, username, status, ip="N/A"):
        try:
            with self.table_lock:
                row_position = -1
                for row in range(self.status_table.rowCount()):
                    if self.status_table.item(row, 0) and self.status_table.item(row, 0).text() == username:
                        row_position = row
                        break
                if row_position == -1:
                    row_position = self.status_table.rowCount()
                    self.status_table.insertRow(row_position)
                    self.status_table.setItem(row_position, 0, QTableWidgetItem(username))
                self.status_table.setItem(row_position, 1, QTableWidgetItem(status))
                self.status_table.setItem(row_position, 2, QTableWidgetItem(ip))
                self.status_table.setItem(row_position, 3, QTableWidgetItem(time.strftime("%Y-%m-%d %H:%M:%S")))
                self.status_table.scrollToItem(self.status_table.item(row_position, 0))
                logging.info(f"Đã cập nhật trạng thái cho {username}: {status}, IP: {ip}")
                # Cập nhật transaction_status dựa trên trạng thái
                with status_lock:
                    if "Giao dịch thành công" in status:
                        transaction_status[username] = True
                    elif "Lỗi" in status or "thất bại" in status:
                        transaction_status[username] = False
        except Exception as e:
            logging.error(f"Lỗi khi cập nhật status_table cho {username}: {str(e)}")

    def load_config(self):
        try:
            if not CONFIG_FILE.exists():
                default_config = {
                    'link': '',
                    'proxy': '',
                    'account': '',
                    'mode': 'Không nạp',
                    'phone': '',
                    'maxweb': '5',
                    'api_mode': 'SMOTP'
                }
                with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                    json.dump(default_config, f, indent=4, ensure_ascii=False)
                logging.info("Đã tạo config.json với giá trị mặc định.")
            else:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    self.link_input.setText(config.get('link', ''))
                    self.proxy_input.setPlainText(config.get('proxy', ''))
                    self.account_input.setPlainText(config.get('account', ''))
                    mode = config.get('mode', 'Không nạp')
                    if mode in ["Không nạp", "VTP", "QR", "CK nhanh"]:
                        self.mode_combo.setCurrentText(mode)
                    self.phone_input.setText(config.get('phone', ''))
                    self.maxweb_input.setText(config.get('maxweb', '5'))
                    api_mode = config.get('api_mode', 'SMOTP')
                    if api_mode in ["SMS", "SMOTP"]:
                        self.api_combo.setCurrentText(api_mode)
        except Exception as e:
            logging.error(f"Lỗi khi tải hoặc tạo config: {e}")

    def save_config(self):
        try:
            config = {
                'link': self.link_input.text().strip(),
                'proxy': self.proxy_input.toPlainText().strip(),
                'account': self.account_input.toPlainText().strip(),
                'mode': self.mode_combo.currentText(),
                'phone': self.phone_input.text().strip(),
                'maxweb': self.maxweb_input.text().strip(),
                'api_mode': self.api_combo.currentText()
            }
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
        except Exception as e:
            logging.error(f"Lỗi khi lưu config: {e}")

    def close_all_browsers(self):
        global active_browsers
        logging.info("Đang đóng tất cả trình duyệt Chrome...")
        with file_lock:
            if active_browsers:
                logging.info(f"Đang đóng {len(active_browsers)} trình duyệt...")

                def close_browser(driver):
                    try:
                        driver.quit()
                        logging.info(f"Đã đóng trình duyệt: {driver}")
                    except Exception as e:
                        logging.error(f"Lỗi khi đóng trình duyệt {driver}: {e}")

                with concurrent.futures.ThreadPoolExecutor(max_workers=len(active_browsers)) as executor:
                    executor.map(close_browser, active_browsers)
            active_browsers.clear()
            logging.info("Đã xóa danh sách trình duyệt đang hoạt động.")
            for proc in psutil.process_iter(['name']):
                try:
                    if proc.info['name'].lower() == 'chromedriver.exe':
                        proc.terminate()
                        proc.wait(timeout=3)
                        logging.info(f"Đã kết thúc tiến trình ChromeDriver: {proc.pid}")
                except Exception as e:
                    logging.error(f"Lỗi khi kết thúc tiến trình ChromeDriver {proc.pid}: {e}")
        logging.info("Đã đóng tất cả trình duyệt và dọn dẹp tài nguyên.")

    def reset_program_state(self):
        global total_browsers
        logging.info("Đang reset trạng thái chương trình...")
        try:
            browser_ready_event.clear()
            batch_completion_event.clear()
            stop_event.clear()
            total_browsers = 0
            self.workers = []
            self.proxy_threads = []
            logging.info("Hoàn tất reset trạng thái chương trình.")
        except Exception as e:
            logging.error(f"Lỗi khi reset trạng thái: {e}")
            raise

    def start_automation(self):
        global credentials, total_browsers, status_lock, browser_status, transaction_status
        try:
            logging.info("Bắt đầu dừng tất cả hoạt động trước đó...")
            stop_event.set()
            for worker in self.workers:
                try:
                    worker.stop()
                    logging.info(f"Đã dừng worker cho {worker.username}")
                except Exception as e:
                    logging.error(f"Lỗi khi dừng worker cho {worker.username}: {e}")
            self.workers.clear()
            time.sleep(1)
            stop_event.clear()
            logging.info("Đang đóng tất cả trình duyệt...")
            self.close_all_browsers()
            logging.info("Đang reset trạng thái chương trình...")
            self.reset_program_state()
            logging.info("Đang lưu cấu hình...")
            self.save_config()
            logging.info("Đang xử lý thông tin tài khoản...")

            # Kiểm tra và tải tài khoản thất bại trước
            credentials = self.load_failed_accounts()
            if not credentials:
                # Nếu không có tài khoản thất bại, tải từ đầu vào
                credentials = []
                for line in self.account_input.toPlainText().strip().split('\n'):
                    parts = line.split('|')
                    if len(parts) >= 3:
                        username = parts[0].strip()
                        password = parts[1].strip()
                        amount = parts[2].strip()
                        try:
                            if not amount.isdigit() or int(amount) <= 0:
                                logging.error(f"Số tiền không hợp lệ cho tài khoản {username}: {amount}")
                                continue
                            credentials.append((username, password, amount))
                        except ValueError:
                            logging.error(f"Số tiền không phải số hợp lệ cho tài khoản {username}: {amount}")
                            continue
                    else:
                        logging.error(f"Dòng tài khoản không đúng định dạng: {line}")
                        continue

            # Lọc tài khoản đã thành công
            successful_accounts = get_successful_accounts()
            credentials = [(username, password, amount) for username, password, amount in credentials
                           if username not in successful_accounts]
            num_accounts = len(credentials)
            if not credentials:
                logging.error("Không còn tài khoản hợp lệ (tất cả đã thành công hoặc không hợp lệ).")
                QMessageBox.warning(self, "Lỗi", "Không có tài khoản hợp lệ để xử lý!")
                self.run_button.setEnabled(True)
                self.run_button.setVisible(True)
                return
            logging.info(f"Số tài khoản còn lại: {num_accounts}")

            # Kiểm tra số lượng web tối đa
            try:
                batch_size = int(self.maxweb_input.text().strip())
                if batch_size <= 0 or batch_size > 20:
                    logging.error("Số lượng web tối đa phải từ 1 đến 20.")
                    QMessageBox.warning(self, "Lỗi", "Số lượng web tối đa phải từ 1 đến 20!")
                    self.run_button.setEnabled(True)
                    self.run_button.setVisible(True)
                    return
            except ValueError:
                logging.error("Số lượng web tối đa phải là số hợp lệ.")
                QMessageBox.warning(self, "Lỗi", "Số lượng web tối đa phải là số hợp lệ!")
                self.run_button.setEnabled(True)
                self.run_button.setVisible(True)
                return

            # Giới hạn số tài khoản tối đa
            max_webs = 1000
            credentials = credentials[:max_webs]
            logging.info("Đang khởi tạo trạng thái trình duyệt...")
            with status_lock:
                browser_status.clear()
                transaction_status.clear()

            # Kiểm tra link
            base_link = self.link_input.text().strip()
            mode = self.mode_combo.currentText()
            phone_number = self.phone_input.text().strip()
            api_mode = self.api_combo.currentText()
            if not base_link:
                logging.error("Không có link cơ bản được cung cấp.")
                QMessageBox.warning(self, "Lỗi", "Vui lòng nhập link cơ bản!")
                self.run_button.setEnabled(True)
                self.run_button.setVisible(True)
                return
            link = base_link + "/Account/LoginToSupplier?supplierType=26&gId=792&cId=2"

            # Xử lý thông tin proxy
            proxies = {}
            proxy_list = []
            proxy_status = {}
            logging.info("Đang xử lý thông tin proxy...")
            for line in self.proxy_input.toPlainText().strip().split('\n'):
                parts = line.split('|')
                if len(parts) == 2:
                    api_key = parts[0].strip()
                    proxy = parts[1].strip()
                    proxies[api_key] = proxy
                    proxy_list.append((api_key, proxy))
                    proxy_status[proxy] = False
                else:
                    logging.error(f"Dòng proxy không đúng định dạng: {line}")
            if not proxies:
                logging.error("Không có proxy hợp lệ. Vui lòng kiểm tra đầu vào proxy.")
                QMessageBox.warning(self, "Lỗi", "Không có proxy hợp lệ. Vui lòng kiểm tra!")
                self.run_button.setEnabled(True)
                self.run_button.setVisible(True)
                return
            num_proxies = len(proxies)
            logging.info(f"Số proxy: {num_proxies}")

            # Đổi và kiểm tra IP ban đầu cho tất cả proxy
            logging.info("Đang khởi tạo proxy bằng cách đổi IP ban đầu...")
            self.change_all_proxy_ips(proxy_list, proxy_status)
            if not any(proxy_status.values()):
                logging.error("Không có proxy nào hoạt động sau khi đổi IP ban đầu")
                QMessageBox.warning(self, "Lỗi", "Không có proxy nào hoạt động. Vui lòng kiểm tra proxy!")
                self.run_button.setEnabled(True)
                self.run_button.setVisible(True)
                return

            # Khởi động thread run_sequential
            logging.info("Đang khởi động thread run_sequential...")
            run_thread = threading.Thread(
                target=self.run_sequential,
                args=(
                num_accounts, batch_size, credentials, proxy_list, proxy_status, link, mode, phone_number, num_proxies,
                api_mode),
                daemon=True
            )
            self.run_thread = run_thread
            run_thread.start()
            run_thread.join(timeout=1)
            if not run_thread.is_alive():
                logging.error("Thread run_sequential không khởi động hoặc kết thúc sớm.")
                QMessageBox.warning(self, "Lỗi", "Không thể khởi động quá trình tự động hóa!")
                self.run_button.setEnabled(True)
                self.run_button.setVisible(True)

        except Exception as e:
            logging.error(f"Lỗi trong start_automation: {e}")
            QMessageBox.critical(self, "Lỗi", f"Đã xảy ra lỗi: {str(e)}")
            self.run_button.setEnabled(True)
            self.run_button.setVisible(True)

    def run_batch(self, batch_credentials, batch_index, proxy_list, proxy_status, link, mode, phone_number, num_proxies,
                  api_mode):
        global total_browsers
        if stop_event.is_set():
            logging.info("Dừng batch do sự kiện dừng.")
            return
        try:
            logging.info(f"Bắt đầu batch {batch_index} với {len(batch_credentials)} tài khoản")
            total_browsers = len(batch_credentials)
            with status_lock:
                browser_status.clear()
                transaction_status.clear()
                for username, _, _ in batch_credentials:
                    browser_status[username] = False
                    transaction_status[username] = False
            batch_workers = []
            browser_width = 900
            browser_height = 900
            max_browsers_per_row = 20
            offset = -650
            vertical_offset = 50

            # Lọc proxy hoạt động
            active_proxies = [(api_key, proxy) for api_key, proxy in proxy_list if proxy_status.get(proxy, False)]
            if not active_proxies:
                logging.error(f"Batch {batch_index}: Không có proxy hoạt động, bỏ qua batch")
                return

            for idx, (username, password, amount) in enumerate(batch_credentials):
                if stop_event.is_set():
                    logging.info(f"Dừng khởi tạo worker cho {username} do sự kiện dừng.")
                    break
                proxy_index = idx % len(active_proxies)
                api_key, proxy = active_proxies[proxy_index]
                col = idx % max_browsers_per_row
                row = 0
                x_pos = col * (browser_width + offset)
                y_pos = row * (browser_height + vertical_offset)
                worker = Worker(link, username, password, proxy, api_key, mode, amount, phone_number, api_mode,
                                browser_width, browser_height, x_pos, y_pos)
                worker.status_updated.connect(self.add_status_to_table)
                batch_workers.append(worker)
                with file_lock:
                    self.workers.append(worker)
                worker.start()
                time.sleep(5)
            for worker in batch_workers:
                worker.wait()
            logging.info(f"Batch {batch_index} đã hoàn tất")
        except Exception as e:
            logging.error(f"Lỗi trong batch {batch_index}: {e}")
            self.close_all_browsers()
        finally:
            batch_completion_event.set()


    def run_sequential(self, num_accounts, batch_size, credentials, proxy_list, proxy_status, link, mode, phone_number,
                       num_proxies, api_mode):
        try:
            logging.info("Bắt đầu xử lý batch tuần tự...")
            max_retries = 3
            retry_count = 0

            # Lấy danh sách tài khoản đã thành công ngay từ đầu
            successful_accounts = get_successful_accounts()

            while retry_count <= max_retries:
                failed_credentials = []
                if retry_count > 0:
                    logging.info(f"Bắt đầu thử lại lần {retry_count} với {len(credentials)} tài khoản thất bại")
                    self.change_all_proxy_ips(proxy_list, proxy_status)
                    logging.info("Đã đổi IP cho tất cả proxy trước khi thử lại")

                # Lọc lại tài khoản để đảm bảo không xử lý tài khoản đã thành công
                credentials = [(username, password, amount) for username, password, amount in credentials
                               if username not in successful_accounts]

                if not credentials:
                    logging.info("Không còn tài khoản nào để xử lý (tất cả đã thành công hoặc không hợp lệ).")
                    break

                if api_mode == "Nạp Tay":
                    # Chế độ Nạp Tay: Chạy song song, tái sử dụng proxy ngay khi Worker hoàn thành
                    logging.info("Chế độ Nạp Tay: Chạy song song, tái sử dụng proxy ngay khi Worker hoàn thành.")
                    active_proxies = [(api_key, proxy) for api_key, proxy in proxy_list if
                                      proxy_status.get(proxy, False)]
                    if not active_proxies:
                        logging.error("Không có proxy hoạt động cho Nạp Tay, bỏ qua.")
                        break

                    browser_width = 900
                    browser_height = 900
                    max_browsers_per_row = 10
                    offset = -650
                    vertical_offset = 50
                    max_concurrent_workers = len(active_proxies)  # Số Worker tối đa = số proxy

                    # Danh sách Worker đang chạy và proxy đang sử dụng
                    running_workers = []
                    available_proxies = active_proxies.copy()
                    credential_index = 0

                    while credential_index < len(credentials) or running_workers:
                        if stop_event.is_set():
                            logging.info(f"Dừng xử lý do sự kiện dừng.")
                            break

                        # Khởi tạo Worker mới nếu có proxy sẵn sàng và còn tài khoản
                        while len(running_workers) < max_concurrent_workers and credential_index < len(
                                credentials) and available_proxies:
                            username, password, amount = credentials[credential_index]
                            api_key, proxy = available_proxies.pop(0)  # Lấy proxy đầu tiên sẵn sàng
                            col = credential_index % max_browsers_per_row
                            row = 0
                            x_pos = col * (browser_width + offset)
                            y_pos = row * (browser_height + vertical_offset)

                            worker = Worker(link, username, password, proxy, api_key, mode, amount, phone_number,
                                            api_mode,
                                            browser_width, browser_height, x_pos, y_pos)
                            worker.status_updated.connect(self.add_status_to_table)
                            with file_lock:
                                self.workers.append(worker)
                            worker.start()
                            running_workers.append((worker, proxy, api_key))
                            logging.info(f"Khởi tạo Worker cho tài khoản {username} với proxy {proxy}.")
                            credential_index += 1

                        # Kiểm tra trạng thái các Worker đang chạy
                        for worker_info in running_workers[:]:
                            worker, proxy, api_key = worker_info
                            if not worker.isRunning():
                                # Worker hoàn thành, đổi IP cho proxy
                                logging.info(
                                    f"Tài khoản {worker.username} hoàn thành (thành công: {transaction_status.get(worker.username, False)}), đổi IP cho proxy {proxy}.")
                                proxy_handler = ProxyHandler(api_key)
                                success, new_ip = proxy_handler.change_ip_using_api(proxy, retry_limit=3)
                                if success:
                                    proxy_status[proxy] = True
                                    logging.info(f"Đổi IP thành công cho proxy {proxy}: {new_ip}")
                                else:
                                    logging.warning(f"Không đổi được IP cho proxy {proxy}.")

                                # Thêm proxy trở lại danh sách sẵn sàng
                                available_proxies.append((api_key, proxy))
                                # Kiểm tra trạng thái để thêm vào danh sách thất bại nếu cần
                                # with status_lock:
                                #     if not transaction_status.get(worker.username, False):
                                #         logging.info(
                                #             f"Tài khoản {worker.username} thất bại/timeout, thêm vào danh sách thử lại.")
                                #         for cred in credentials:
                                #             if cred[0] == worker.username:
                                #                 failed_credentials.append(cred)
                                #                 break
                                running_workers.remove(worker_info)

                        time.sleep(1)  # Tránh kiểm tra quá nhanh

                else:
                    # Các mode khác: Giữ nguyên chạy batch song song
                    for batch_index in range((len(credentials) + batch_size - 1) // batch_size):
                        start_idx = batch_index * batch_size
                        end_idx = min(start_idx + batch_size, len(credentials))
                        batch_credentials = credentials[start_idx:end_idx]
                        batch_completion_event.clear()
                        batch_thread = threading.Thread(
                            target=self.run_batch,
                            args=(
                            batch_credentials, batch_index + 1, proxy_list, proxy_status, link, mode, phone_number,
                            num_proxies, api_mode),
                            daemon=True
                        )
                        batch_thread.start()
                        batch_completion_event.wait(timeout=1800)
                        if stop_event.is_set():
                            logging.info("Dừng xử lý tuần tự do sự kiện dừng.")
                            break
                        with status_lock:
                            batch_usernames = [username for username, _, _ in batch_credentials]
                            all_success = all(transaction_status.get(username, False) for username in batch_usernames)
                        if all_success:
                            logging.info(f"Tất cả tài khoản trong batch {batch_index + 1} thành công, chờ 5 giây...")
                            time.sleep(5)
                        else:
                            logging.info(f"Batch {batch_index + 1} có tài khoản thất bại, chờ tối đa 1800 giây...")
                        self.change_all_proxy_ips(proxy_list, proxy_status)
                        logging.info(f"Đã đổi IP cho tất cả proxy sau batch {batch_index + 1}")

                # Cập nhật danh sách tài khoản thành công sau mỗi batch hoặc sequential
                successful_accounts = get_successful_accounts()

                # Xây dựng danh sách tài khoản thất bại
                with status_lock:
                    for username, password, amount in credentials:
                        if username not in successful_accounts and not transaction_status.get(username, False):
                            failed_credentials.append((username, password, amount))
                            logging.info(f"Tài khoản {username} thất bại, sẽ thử lại")

                # Lưu tài khoản thất bại
                self.save_failed_accounts(failed_credentials)

                if not failed_credentials or stop_event.is_set():
                    logging.info("Không có tài khoản nào cần thử lại hoặc bị dừng.")
                    if not failed_credentials:
                        try:
                            os.remove('failed_accounts.txt')
                            logging.info("Đã xóa failed_accounts.txt vì không còn tài khoản thất bại.")
                        except FileNotFoundError:
                            pass
                        except Exception as e:
                            logging.error(f"Lỗi khi xóa failed_accounts.txt: {e}")
                    break

                credentials = failed_credentials
                retry_count += 1
                if retry_count <= max_retries:
                    logging.info(f"Chuẩn bị thử lại {len(credentials)} tài khoản thất bại trong lần {retry_count}")

            with status_lock:
                success_count = sum(1 for username in transaction_status if transaction_status.get(username, False))
                failure_count = len(transaction_status) - success_count
            msg = QMessageBox()
            msg.setWindowTitle("Hoàn tất xử lý")
            msg.setText(
                f"Tất cả tài khoản đã hoàn thành xử lý.\nThành công: {success_count}\nThất bại: {failure_count}")
            msg.setIcon(QMessageBox.Information)
            msg.setStandardButtons(QMessageBox.Ok)
            logging.info("Hiển thị QMessageBox thông báo hoàn tất xử lý")
            msg.exec_()

            logging.info("Tất cả batch và thử lại hoàn tất hoặc dừng. Kích hoạt lại nút chạy.")
            self.run_button.setEnabled(True)
            self.run_button.setVisible(True)
        except Exception as e:
            logging.error(f"Lỗi trong run_sequential: {e}")
            self.run_button.setEnabled(True)
            self.run_button.setVisible(True)




    def change_all_proxy_ips(self, proxy_list, proxy_status):
        logging.info("Đang thay đổi IP cho tất cả proxy...")

        def change_ip_for_proxy(api_key, proxy):
            proxy_handler = ProxyHandler(api_key)
            success, new_ip = proxy_handler.change_ip_using_api(proxy, retry_limit=3)
            if success and check_proxy(proxy, proxy_handler, max_retries=1):
                with file_lock:
                    proxy_status[proxy] = True
                logging.info(f"Proxy {proxy} đã đổi IP thành công và hoạt động với IP: {new_ip}")
            else:
                with file_lock:
                    proxy_status[proxy] = False
                logging.error(f"Proxy {proxy} không đổi được IP hoặc không hoạt động")

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(proxy_list)) as executor:
            futures = [executor.submit(change_ip_for_proxy, api_key, proxy) for api_key, proxy in proxy_list]
            concurrent.futures.wait(futures)

        # Bỏ kiểm tra tất cả proxy phải hoạt động
        active_proxy_count = sum(1 for _, proxy in proxy_list if proxy_status.get(proxy, False))
        logging.info(f"Hoàn tất đổi IP: {active_proxy_count}/{len(proxy_list)} proxy hoạt động")

    def save_failed_accounts(self, failed_credentials):
        try:
            # Kiểm tra lại danh sách tài khoản thành công
            successful_accounts = get_successful_accounts()
            filtered_failed_credentials = [(username, password, amount) for username, password, amount in
                                           failed_credentials
                                           if username not in successful_accounts]

            if not filtered_failed_credentials:
                try:
                    os.remove('failed_accounts.txt')
                    logging.info("Đã xóa failed_accounts.txt vì không còn tài khoản thất bại.")
                except FileNotFoundError:
                    pass
                return

            with open('failed_accounts.txt', 'w', encoding='utf-8') as f:
                for username, password, amount in filtered_failed_credentials:
                    f.write(f"{username}|{password}|{amount}\n")
            logging.info(f"Đã lưu {len(filtered_failed_credentials)} tài khoản thất bại vào failed_accounts.txt")
        except Exception as e:
            logging.error(f"Lỗi khi lưu tài khoản thất bại: {e}")

    def load_failed_accounts(self):
        failed_credentials = []
        try:
            with open('failed_accounts.txt', 'r', encoding='utf-8') as f:
                for line in f:
                    parts = line.strip().split('|')
                    if len(parts) >= 3:
                        username = parts[0].strip()
                        password = parts[1].strip()
                        amount = parts[2].strip()
                        if amount.isdigit() and int(amount) > 0:
                            failed_credentials.append((username, password, amount))
            logging.info(f"Đã tải {len(failed_credentials)} tài khoản thất bại từ failed_accounts.txt")
        except FileNotFoundError:
            logging.info("Không tìm thấy failed_accounts.txt, sử dụng danh sách tài khoản từ đầu vào.")
        except Exception as e:
            logging.error(f"Lỗi khi tải tài khoản thất bại: {e}")
        return failed_credentials

    def closeEvent(self, event):
        self.save_config()
        self.close_all_browsers()
        event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()  # type: ignore
    window.show()
    sys.exit(app.exec_())