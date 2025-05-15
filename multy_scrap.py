import pandas as pd
from single_scrap import scrap_webstore_single
import time

def scrap_webstore_multy(df: pd.DataFrame, url_column: str = "url") -> pd.DataFrame:
    results = []
    
    for _, row in df.iterrows():
        url = row[url_column]
        if pd.notna(url):  
            stock, price, remove_flag = scrap_webstore_single(url)
            results.append([url, stock, price, remove_flag])
        else:
            results.append([url, "Invalid URL", None])
    
    # Create DataFrame with URLs, Stock, and Price
    results_df = pd.DataFrame(results, columns=['url', 'Stock', 'Price', 'Remove'])
    
    return results_df


# urls = [
#     "https://www.webstaurantstore.com/regency-nsf-mobile-green-wire-security-cage-kit-24-x-60-x-69/460GSC2460KM.html",  # test_ok
#     "https://www.webstaurantstore.com/metro-super-erecta-30-x-72-x-80-gray-mobile-shelving-unit-kit/4613072NK4M75.html",  # test_out_of_stock
#     "https://www.webstaurantstore.com/lavex-49-1-4-x-41-1-4-x-4-heavy-duty-gaylord-lid-bundle/442GLTW492541254LID.html",  # test_many_buy
#     "https://www.webstaurantstore.com/lancaster-table-seating-18-x-60-granite-white-heavy-duty-blow-molded-plastic-folding-table/384YCZ1860.html"  # test_table_element
# ]

# df_test = pd.DataFrame({'url':urls*25})

# print(scrap_webstore_multy(df_test))