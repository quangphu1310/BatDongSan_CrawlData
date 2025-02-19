import requests
import pymysql
import os
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pathlib import Path
import urllib3
from datetime import datetime
from fastapi import FastAPI
import pyodbc

env_path = Path(".") / ".env"
load_dotenv(dotenv_path=env_path)

# app = FastAPI()

list_url = [
    "https://bds123.vn/nha-dat-cho-thue-ha-noi.html",
    "https://bds123.vn/nha-dat-cho-thue-da-nang.html",
    "https://bds123.vn/nha-dat-cho-thue-ho-chi-minh.html"
]
list_location = ['Ha Noi', 'Da Nang', 'TP.HCM']

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Content-Type': 'text/html; charset=utf-8',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Kết nối tới SQLServer database
def connect_to_db():
    try:
        conn = pyodbc.connect(
            "DRIVER={SQL Server};"
            "SERVER=QUANGPHU\\QUANGPHU;"  # Thay bằng tên máy chủ SQL Server của bạn
            "DATABASE=real_estate;"  # Tên database
            "UID=sa;"  # Tên user (nếu dùng Windows Authentication thì bỏ UID & PWD)
            "PWD=123456;"  # Mật khẩu
            "TrustServerCertificate=yes;"
        )
        print("Kết nối thành công đến SQL Server")
        return conn
    except Exception as e:
        print(f"Lỗi kết nối tới SQL Server: {e}")
        raise

def insert_data_to_db(conn, title, price, square, rooms, address, district, city, posted_date, image_urls):
    try:
        # Lấy districtId từ bảng District
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id FROM District WHERE [name] = ? AND ProvinceId = (
                SELECT id FROM Province WHERE [name] = ?
            )
        """, (district, city))
        
        result = cursor.fetchone()
        if result:
            districtId = result[0]
        else:
            print(f"Không tìm thấy districtId cho quận {district} và thành phố {city}")
            return

        # Chèn dữ liệu vào bảng Properties2
        sql_command = """
            INSERT INTO Properties2 (title, price, square, rooms, address, districtId, posted_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(
            sql_command,
            (title, price, square, rooms, address, districtId, posted_date)
        )
        conn.commit()
        print("Dữ liệu đã được chèn vào bảng Properties2.")

        # Lấy PropertyId của bản ghi vừa chèn vào
        cursor.execute("SELECT id FROM Properties2 WHERE title = ?", (title,))
        # cursor.execute("SELECT SCOPE_IDENTITY()")
        property_id = cursor.fetchone()[0]
        
        if property_id is None:
            print("Không lấy được PropertyId.")
            return
        
        print(f"PropertyId vừa chèn vào là: {property_id}")
        
        # Chèn các URL ảnh vào bảng PropertyImages
        if image_urls:
            for img_url in image_urls:
                cursor.execute("""
                    INSERT INTO PropertyImages (ImageUrl, PropertyId)
                    VALUES (?, ?)
                """, (img_url, property_id))
            conn.commit()
            print(f"{len(image_urls)} ảnh đã được chèn vào bảng PropertyImages.")
        
    except Exception as e:
        print(f"Lỗi SQLServer: {e}")
        conn.rollback()


def getDataDetail(detail_url):
    response = requests.get(detail_url, verify=False)
    soup = BeautifulSoup(response.text, "html.parser")
    formatted_date = address = district = city = None
    image_urls = []
    
    rows = soup.find_all('tr')
    if len(rows) >= 6:
        date_cell = rows[5].find('time')
        if date_cell:
            date_value = date_cell.text.strip()
            date_str = date_value.split(" ")[-1]
        
            # Chuyển đổi sang định dạng YYYY-MM-DD
            date_obj = datetime.strptime(date_str, '%d/%m/%Y')
            formatted_date = date_obj.strftime('%Y-%m-%d')
        else:
            print("Không tìm thấy posted_date")
    else:
        print("Không tìm thấy posted_date")
        
    address, district, city = getFullAddress(soup.find('p', class_='d-flex align-items-center mt-0 mb-3'))

    # Lấy tất cả các ảnh từ HTML
    image_div = soup.find('div', class_='post-images')
    if image_div:
        # Lấy tất cả các thẻ img trong div
        images = image_div.find_all('img')
        for img in images:
            # Lấy src của ảnh
            src = img.get('data-src')
            if src:
                image_urls.append(src)
    
    # In ra các URL của ảnh
    for img_url in image_urls:
        print(img_url)

    return formatted_date, address, district, city, image_urls


def crawl_data_info(conn, card, detail_url):
    try:
        price = getPrice(card.find('span', class_="price").text.strip())
        if price == 0:
            return
        
        title = card.find('a').get("title").strip()
        square, rooms, bathrooms = getInformationFeatures(card.find('div', class_='info-features'))
        if square == None:
            return
        
        # Lấy thông tin chi tiết
        posted_date, address, district, city, image_urls = getDataDetail(detail_url)
        
        list_city = ["Hà Nội", "Đà Nẵng", "Hồ Chí Minh"]
        if city in list_city:
            # Chèn dữ liệu vào cơ sở dữ liệu
            insert_data_to_db(conn, title, price, square, rooms, address, district, city, posted_date, image_urls)
        
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi lấy dữ liệu chi tiết: {e}")



def crawl_data_main():
    conn = connect_to_db()
    location = 0  # Biến theo dõi tỉnh hiện tại
    page = 1  # Biến theo dõi trang hiện tại
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    for page in range(1, 3):  # Lặp qua từng trang
        # Lặp qua từng URL
        for base_url in list_url:
            full_url = f"{base_url}?page={page}"  # Ghép URL đầy đủ
            print(f"Đang cào dữ liệu ở trang {page} của {base_url}...")
            response = requests.get(full_url, headers=headers, verify=False)  # Gửi yêu cầu GET
            soup = BeautifulSoup(response.text, "html.parser")
            # Tìm tất cả các thẻ chứa thông tin bất động sản
            container_cards = soup.find_all("li", class_="item box-shadow border-radius vip30 clearfix") + \
                            soup.find_all("li", class_="item box-shadow border-radius vip20 clearfix") + \
                            soup.find_all("li", class_="item box-shadow border-radius vip40 clearfix") + \
                            soup.find_all("li", class_="item box-shadow border-radius normal clearfix") + \
                            soup.find_all("li", class_="item box-shadow border-radius free clearfix")
            
            if not container_cards:
                print(f"Không còn dữ liệu ở trang {page}, dừng cào.")
                break

            # Cào dữ liệu cho từng thẻ
            for card in container_cards:
                detail_url = card.find("a").get("href")
                crawl_data_info(conn, card, f"{list_url[location]}{detail_url}")
    conn.close()

def getPrice(priceStr):
    if priceStr is None or priceStr == "Thỏa thuận":
        return 0
    
    parts = priceStr.split(" ")
    if len(parts) > 1:
        if parts[-1] == "tỷ":
            price = float(parts[0]) * 1_000_000_000
            return price
        elif parts[-1] == "triệu/tháng":
            price = float(parts[0]) * 1_000_000
            return price

    return 0

def getInformationFeatures(info):
    area = bedrooms = bathrooms = None
    items = info.find_all('span', class_="feature-item")
    
    if len(items) >= 1:
        area = items[0].text.strip().split()[0]
    if len(items) >= 2:
        bedrooms = items[1].text.strip().split()[0]
    if len(items) >= 3:
        bathrooms = items[2].text.strip().split()[0]
        
    if bedrooms == None:
        bedrooms = 1

    return area, bedrooms, bathrooms

def getFullAddress(address):
    mAddress = district = city = None
    
    if address:
        full_address = address.text.strip()
        parts = [part.strip() for part in full_address.split(',')]
        keep_province_list = {"Quận 1", "Quận 2", "Quận 3", "Quận 4", "Quận 5", "Quận 6", 
                      "Quận 7", "Quận 8", "Quận 9", "Quận 10", "Quận 11", "Quận 12"}
        
        if len(parts) >= 3:
            city = parts[-1]
            district = parts[-2]
            mAddress = ', '.join(parts[:-2])
            
            # Loại bỏ "Quận" hoặc "Huyện" trong giá trị district, ngoại trừ các quận ngoại lệ
            if district not in keep_province_list:
                if "Quận" in district:
                    district = district.replace("Quận", "").strip()
                elif "Huyện" in district:
                    district = district.replace("Huyện", "").strip()
                else:
                    district = district
        else:
            print("Địa chỉ không đủ thông tin")
    else:
        print("Địa chỉ không đủ thông tin")
        
    return mAddress, district, city

if __name__ == "__main__":
    crawl_data_main()