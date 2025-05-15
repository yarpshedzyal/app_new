import requests
from bs4 import BeautifulSoup
import re
import random
import time

proxies_list = [    "http://vps_ua_amz2025:unqNhf25k8@46.3.134.108:50100",
                    "http://vps_ua_amz2025:unqNhf25k8@149.126.228.119:50100",
                    "http://vps_ua_amz2025:unqNhf25k8@104.219.170.70:50100",
                    "http://vps_ua_amz2025:unqNhf25k8@216.180.245.77:50100",
                    "http://vps_ua_amz2025:unqNhf25k8@150.241.243.79:50100",
                    "http://vps_ua_amz2025:unqNhf25k8@96.62.127.254:50100", 
                    "http://vps_ua_amz2025:unqNhf25k8@95.164.206.175:50100",
                    "http://vps_ua_amz2025:unqNhf25k8@193.41.68.197:50100", 
                    "http://vps_ua_amz2025:unqNhf25k8@64.226.156.235:50100",
                    "http://vps_ua_amz2025:unqNhf25k8@94.131.48.220:50100",
                                                                            ]


def get_random_proxie():
    return{'http': random.choice(proxies_list), 'https': random.choice(proxies_list)}

def get_next_proxy(used):
    available = [p for p in proxies_list if p not in used]
    if not available:
        return None
    proxy = random.choice(available)
    return {'http': proxy, 'https': proxy}

def get_minimum_buy_number(soup):
    min_must_text_element = soup.find("p", {"class": "min-must-text"})
    if min_must_text_element:
        minimum_buy_number = re.search(r"\d+", min_must_text_element.text)
        if minimum_buy_number:
            return int(minimum_buy_number.group())
    return None

def clean_price_string(price_str):
    parts = price_str.split(".", 1)
    if len(parts) == 2:
        cleaned_price = f"{parts[0]}.{parts[1][:2]}"
    else:
        cleaned_price = price_str.replace(".", "")
    cleaned_price = re.sub(r"[^\d.]", "", cleaned_price)
    return cleaned_price

def scrap_webstore_single(url:str, max_attempts=5):
    stock_inner: str = 'Out'
    price_inner: float = 0
    attempts = 0
    used_proxies = set()
    remove_from_file = False

    while attempts < max_attempts:
        proxy = get_next_proxy(used_proxies)
        if not proxy:
            print("No proxies left to try.")
            break

        try:
            response = requests.get(url, proxies=proxy, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")
            price_element = soup.select_one("#priceBox > div.pricing > div > p > span.price")
            svg_element = soup.find("svg", {"class": "block mx-auto align-middle"})
            phrase_unavailable = "This Product is no longer available"
            phrase_out_of_stock = "Notify me when this product is back in stock"
            phrase_unavailable_2 = "This product is no longer available"
            minimum_buy = get_minimum_buy_number(soup)
            table_element = soup.select_one("table.table.table-bordered")
            phrase_works_with = 'Works With'
            sale_element = soup.select_one('#priceBox > div.pricing > p.sale-price > span.text-black.font-bold.bg-yellow-400.rounded-sm.antialiased.mr-1.mt-0\.5.px-3\/4.py-0\.5.text-sm')
            was_price_element = soup.select_one("p.was-price")
            buybox_new_14_05_2024 = soup.select_one('#priceBox > div.pricing > div.plus-member.plus-member-override.plus-member--plus > div > div.plus-member__text.plus-member__price > p > span')
            bug_01_06_2024 = soup.select_one('#priceBox > div.pricing > table > tbody > tr > td')
            buybox_member_price_plus_05_15_25 = soup.select_one('#priceBox > div.pricing.relative.z-0 > div > div.plus-member.plus-member-override.plus-member--plus > div > div.plus-member__price.plus-member__price--plus-member.flex.justify-center.flex-col > p > span')

            if svg_element or (phrase_unavailable in soup.get_text()) or (phrase_out_of_stock in soup.get_text()) or (phrase_unavailable_2 in soup.get_text()):
                stock_inner = "Out"
            else:
                stock_inner = "In"

            if phrase_unavailable in soup.get_text() or phrase_unavailable_2 in soup.get_text():
                remove_from_file = True

            if table_element:
                rows = table_element.select("tbody tr")
                last_th = None
                last_td = None
                for row in rows:
                    th = row.select_one("th").text
                    td = row.select_one("td").text.strip()
                    last_th = th
                    last_td = td

                if last_th and last_td:
                    filtered_td = re.sub(r'[^\d.]', '', last_td)
                    price_inner = clean_price_string(filtered_td)
                else:
                    return "Table has no rows or data."

            else:
                if price_element:
                        price_inner = price_element.text.strip().replace("$", "").replace(",", "")
                        filtered_price = re.sub(r'[^\d.]', '', price_inner)
                        price_inner = clean_price_string(filtered_price)
                else:
                    print("Price element not found.")

            if phrase_works_with in soup.get_text() and not table_element:
                price_element = soup.select_one('#priceBox > div.pricing > p > span')
                if price_element:
                    price_inner = price_element.text.strip().replace("$", "").replace(",", "")
                    filtered_price = re.sub(r'[^\d.]', '', price_inner)
                    price_inner = clean_price_string(filtered_price)      

            if sale_element and not table_element:
                price_element = soup.select_one('#priceBox > div.pricing > p.sale-price > span:nth-child(2)')
                if price_element:
                    price_inner = price_element.text.strip().replace("$", "").replace(",", "")
                    filtered_price = re.sub(r'[^\d.]', '', price_inner)
                    price_inner = clean_price_string(filtered_price)    

            if was_price_element:
                price_inner = was_price_element.text.strip().replace("$", "").replace(",", "")
                filtered_price = re.sub(r'[^\d.]', '', price_inner)
                price_inner = clean_price_string(filtered_price)  

            if buybox_new_14_05_2024:
                price_inner = buybox_new_14_05_2024.text.strip().replace('$', '').replace(',', '')
                filtered_price = re.sub(r'[^\d.]', '', price_inner)
                price_inner = clean_price_string(filtered_price)

            if bug_01_06_2024:
                price_el = soup.select_one('#priceBox > div.pricing > p > span')
                price_inner = price_el.text.strip().replace('$', '').replace(',','')
                filtered_price = re.sub(r'[^\d.]', '', price_inner)
                price_inner = clean_price_string(filtered_price)         

            if buybox_member_price_plus_05_15_25:    
                price_inner = buybox_member_price_plus_05_15_25.text.strip().replace('$', '').replace(',', '')   
                filtered_price = re.sub(r'[^\d.]', '', price_inner)
                price_inner = clean_price_string(filtered_price)

            if minimum_buy:
                price_inner = str(float(price_inner) * minimum_buy)
            if 'search' in url:
                stock_inner = 'Out' 

            print(proxy)    
            print(stock_inner, price_inner)
            return [stock_inner, price_inner, remove_from_file]
    
        except requests.exceptions.ProxyError as e:
            print(f"[ProxyError] {e}")
        except requests.exceptions.SSLError as e:
            print(f"[SSLError] {e}")
        except requests.exceptions.ConnectionError as e:
            print(f"[ConnectionError] {e}")
        except requests.exceptions.RequestException as e:
            print(f"[RequestException] {e}")
        except Exception as e:
            print(f"[Other Error] {e}")

        attempts += 1
        time.sleep(random.uniform(1.5, 3.5))  # random sleep between retries

    print("Max retries exceeded.")
    return [stock_inner, price_inner, remove_from_file]
# print(scrap_webstore_single('https://www.webstaurantstore.com/regency-nsf-mobile-green-wire-security-cage-kit-24-x-60-x-69/460GSC2460KM.html')) #test_ok
# print(scrap_webstore_single('https://www.webstaurantstore.com/metro-super-erecta-30-x-72-x-80-gray-mobile-shelving-unit-kit/4613072NK4M75.html')) #test_out_of_stock
# print(scrap_webstore_single('https://www.webstaurantstore.com/lavex-49-1-4-x-41-1-4-x-4-heavy-duty-gaylord-lid-bundle/442GLTW492541254LID.html')) #test_many_buy
# print(scrap_webstore_single('https://www.webstaurantstore.com/lancaster-table-seating-18-x-60-granite-white-heavy-duty-blow-molded-plastic-folding-table/384YCZ1860.html')) #test_table_element
