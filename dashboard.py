import streamlit as st
import time
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient, UpdateOne

# MongoDB Connection
client = MongoClient('mongodb+srv://' + st.secrets['DB_USERNAME'] + ':' + st.secrets['DB_PASSWORD'] + '@cluster0.at8wb.mongodb.net/')
db = client['FlightScraper']
collection = db['FlightData']

# Create a unique flight key
def generate_flight_key(row):
    parts = [
        row.get('FLIGHT', '').replace(' ', ''),
        row.get('Date', '').replace('-', '_'),
        row.get('TIME', '').replace(':', '').replace(' ', ''),
        row.get('FROM' if 'FROM' in row else 'TO', '').replace(' ', '')
    ]
    return "_".join(filter(None, parts))

# Convert date time
def format_date(date_str):
    try:
        return datetime.strptime(date_str.strip(), "%A, %B %d").replace(year=datetime.today().year).strftime("%d-%m-%Y")
    except:
        return date_str

def save_to_db(df):
    """
    Save or update flight data in MongoDB.
    
    Parameters:
        df (pd.DataFrame): The dataframe with a 'flight_key' column.
    """
    # Remove any columns with empty string as column name
    df = df.loc[:, df.columns.str.strip() != '']
    df.columns = df.columns.str.strip()

    operations = []

    for _, row in df.iterrows():
        flight_key = row.get("flight_key")
        status = row.get("STATUS")

        if not flight_key:
            continue

        # Remove STATUS from insert document to avoid conflict
        full_doc = row.drop(labels=["STATUS"]).to_dict()
        full_doc = {k: v for k, v in full_doc.items() if k and k.strip()}

        operations.append(
            UpdateOne(
                {"flight_key": flight_key},
                {
                    "$set": {"STATUS": status},
                    "$setOnInsert": full_doc
                },
                upsert=True
            )
        )

    if operations:
        result = collection.bulk_write(operations)
        print(f"Inserted: {result.upserted_count}, Updated: {result.modified_count}")

# Scraping function
def scrape(link, city, flight_type):
    option = Options()
    option.add_argument("--disable-infobars")
    option.add_argument("start-maximized")
    option.add_argument("--disable-extensions")
    option.add_argument('--headless=new')  # Optional: run headless in production

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=option)
    driver.get(link)

    # Accept cookie popup
    try:
        cookie_accept_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        cookie_accept_btn.click()
        time.sleep(2)
    except Exception:
        pass

    # Click "Load More" button
    try:
        load_more_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "btn-flights-load"))
        )
        load_more_btn.click()
        time.sleep(3)
    except Exception:
        pass

    # Scroll to load content
    html_chunks = []
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        html = driver.page_source
        if html in html_chunks:
            break
        html_chunks.append(html)

    driver.quit()
    complete_html = "\n".join(html_chunks)
    soup = bs(complete_html, "html.parser")

    # Extract column headers
    headers = []
    header_row = soup.select_one("table.data-table thead tr.hidden-xs.hidden-sm")
    if header_row:
        headers = [th.get_text(strip=True) for th in header_row.find_all("th")]
        headers.insert(0, "Date")  # Add Date column

    data = []
    table = soup.find("table", class_="data-table")
    if table:
        tbody = table.find("tbody")
        current_date = ""
        for row in tbody.find_all("tr"):
            if "row-date-separator" in row.get("class", []):
                date_td = row.find("td")
                if date_td:
                    current_date = date_td.get_text(strip=True)
            elif row.has_attr("data-date"):
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) == len(headers) - 1:
                    data.append([current_date] + cells)

    df = pd.DataFrame(data, columns=headers)
    df = df.applymap(lambda x: x.rstrip('-').strip() if isinstance(x, str) else x)
    df['Date'] = df['Date'].apply(format_date)

    if 'Status' in df.columns:
        df['Status'] = df['Status'].apply(
            lambda x: x.replace("Scheduled", "Scheduled ") \
                    .replace("Estimated", "Estimated ") \
                    .replace("Landed", "Landed ") \
                    .replace("Cancelled", "Cancelled ") \
                    .replace("Delayed", "Delayed ") \
                    if isinstance(x, str) else x
        )
    
    df['flight_key'] = df.apply(generate_flight_key, axis=1)
    df['airport'] = city
    df['flight_type'] = flight_type
    return df


# Streamlit UI
st.set_page_config(page_title="Pakistan Airport Flight Tracker")

st.title("🛬 Airport Flight Tracker")
st.markdown("""
This tool allows you to view real-time **arrivals** and **departures** for major airports in Pakistan.
""")

# City selection
city = st.selectbox("Select City", ["Islamabad", "Karachi", "Faisalabad", "Lahore", "Peshawar"])

# Direction selection
flight_type = st.radio("Select Flight Type", ["Arrivals", "Departures"])

# Scrape button
if st.button("Scrape Flight Data"):
    with st.spinner('Scraping and analyzing data...'):
        if city == "Islamabad":
            place_code = "isb"
        elif city == "Karachi":
            place_code = "khi"
        elif city == "Faisalabad":
            place_code = "lyp"
        elif city == "Lahore":
            place_code = "lhe"
        elif city == "Peshawar":
            place_code = "pew"

        url = "https://www.flightradar24.com/data/airports/" + place_code + "/" + flight_type.lower()
        df = scrape(url, city, flight_type.lower())
        if not df.empty:
            st.success(f"{flight_type} data for {city} loaded successfully!")
            st.dataframe(df, use_container_width=True)
            save_to_db(df)
        else:
            st.warning("No data found.")
