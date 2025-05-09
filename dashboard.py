import streamlit as st
import pandas as pd
from pymongo import MongoClient
import plotly.express as px
import plotly.graph_objects as go

# MongoDB Connection
client = MongoClient(
    'mongodb+srv://' + st.secrets['DB_USERNAME'] + ':' + st.secrets['DB_PASSWORD'] + '@cluster0.at8wb.mongodb.net/')
db = client['FlightScraper']
collection = db['FlightData']

# Streamlit UI
st.set_page_config(page_title="Pakistan Airport Flight Tracker")

st.title("🛬 Airport Flight Tracker")
st.markdown("""
This tool displays **arrivals** and **departures** for major airports in Pakistan — fetched from stored MongoDB data.
""")

# City selection
city = st.selectbox("Select City", ["Islamabad", "Karachi", "Faisalabad", "Lahore", "Peshawar", "Baku", "Kabul"])

# Direction selection
flight_type = st.radio("Select Flight Type", ["Arrivals", "Departures"])

# Fetch button
if st.button("Fetch Flight Data"):
    with st.spinner("Fetching data from MongoDB..."):
        query = {
            "airport": city,
            "flight_type": flight_type.lower()
        }
        records = list(collection.find(query, {"_id": 0}))

        if records:
            df = pd.DataFrame(records)

            st.subheader(f"✈️ Flight Analysis of {city}")

            # Convert Date to datetime for analysis
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'], format="%d-%m-%Y", errors='coerce')

            # 1. Top Departure Cities (for arrivals) or Arrival Cities (for departures)
            col_name = "FROM" if flight_type == "Arrivals" else "TO"
            if col_name in df.columns:
                st.markdown("### 📍 Top Cities")
                st.markdown(f"Shows the top cities **{flight_type.lower()}ing** to or from **{city}**.")

                top_cities = df[col_name].value_counts().nlargest(10).reset_index()
                top_cities.columns = [col_name, "Flights"]
                fig1 = px.bar(top_cities, x=col_name, y="Flights", color="Flights",
                              color_continuous_scale="blues")
                st.plotly_chart(fig1, use_container_width=True)

            # 2. Most Frequent Flights
            if 'FLIGHT' in df.columns:
                st.markdown("### 🛫 Most Frequent Flights")
                st.markdown("Displays the most frequently occurring flight numbers.")

                top_flights = df['FLIGHT'].value_counts().nlargest(10).reset_index()
                top_flights.columns = ["FLIGHT", "Count"]
                fig2 = px.bar(top_flights, x="FLIGHT", y="Count", color="Count", color_continuous_scale="purples")
                st.plotly_chart(fig2, use_container_width=True)

            # 3. Airline Frequency
            if 'AIRLINE' in df.columns:
                st.markdown("### 🏢 Airlines Frequency")
                st.markdown("This chart shows how often each airline operates in the selected category.")

                airline_freq = df['AIRLINE'].value_counts().nlargest(10).reset_index()
                airline_freq.columns = ["AIRLINE", "Count"]
                fig3 = px.pie(airline_freq, names="AIRLINE", values="Count", title="Airline Share")
                st.plotly_chart(fig3, use_container_width=True)

            # # 4. Flight Status
            # if 'STATUS' in df.columns:
            #     st.markdown("### ⏱️ Flight Status Overview")
            #     st.markdown("Breakdown of flight status such as Landed, Scheduled, Delayed, etc.")

            #     status_counts = df['STATUS'].value_counts().reset_index()
            #     status_counts.columns = ["STATUS", "Count"]
            #     fig4 = px.bar(status_counts, x="STATUS", y="Count", color="STATUS", title="Flight Status Distribution")
            #     st.plotly_chart(fig4, use_container_width=True)

            # 5. Flights over time
            if 'Date' in df.columns and not df['Date'].isnull().all():
                st.markdown("### 📅 Flights per Day")
                st.markdown("This line graph shows how many flights occurred per day.")

                daily_counts = df.groupby('Date').size().reset_index(name="Flights")
                fig5 = px.line(daily_counts, x="Date", y="Flights", markers=True)
                st.plotly_chart(fig5, use_container_width=True)

            # Final Data Table
            st.markdown("### 📄 Full Flight Data")
            st.dataframe(df, use_container_width=True)
        else:
            st.warning("No data found in the database for the selected filters.")
