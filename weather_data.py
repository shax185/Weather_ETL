import os
import pandas as pd
from datetime import datetime, timedelta
from meteostat import Stations, Daily
from concurrent.futures import ThreadPoolExecutor

# Set date range
start = datetime(1980, 1, 1)
end = datetime.now() - timedelta(days=1)

# Create output folder
output_dir = 'weather_data'
os.makedirs(output_dir, exist_ok=True)

# Columns to keep
columns_to_keep = ['time', 'tavg', 'tmin', 'tmax', 'prcp', 'snow', 'wspd', 'pres']

capital_coords = [
    ('Afghanistan', ('Kabul', 34.5553, 69.2075)),
    ('Algeria', ('Algiers', 36.7538, 3.0588)),
    ('Angola', ('Luanda', -8.839, 13.2894)),
    ('Argentina', ('Buenos Aires', -34.6037, -58.3816)),
    ('Australia', ('Canberra', -35.2809, 149.13)),
    ('Austria', ('Vienna', 48.2082, 16.3738)),
    ('Bahrain', ('Manama', 26.2235, 50.5876)),
    ('Bangladesh', ('Dhaka', 23.8103, 90.4125)),
    ('Belgium', ('Brussels', 50.8503, 4.3517)),
    ('Bhutan', ('Thimphu', 27.4728, 89.639)),
    ('Brazil', ('Brasília', -15.7939, -47.8828)),
    ('Bulgaria', ('Sofia', 42.6977, 23.3219)),
    ('Burkina Faso', ('Ouagadougou', 12.3714, -1.5197)),
    ('Cameroon', ('Yaoundé', 3.848, 11.5021)),
    ('Canada', ('Ottawa', 45.4215, -75.6995)),
    ('Chile', ('Santiago', -33.4489, -70.6693)),
    ('China', ('Beijing', 39.9042, 116.4074)),
    ('Colombia', ('Bogotá', 4.711, -74.0721)),
    ('Costa Rica', ('San José', 9.9281, -84.0907)),
    ('Croatia', ('Zagreb', 45.815, 15.9819)),
    ('Cyprus', ('Nicosia', 35.1856, 33.3823)),
    ('Denmark', ('Copenhagen', 55.6761, 12.5683)),
    ('Ecuador', ('Quito', -0.1807, -78.4678)),
    ('Egypt', ('Cairo', 30.0444, 31.2357)),
    ('El Salvador', ('San Salvador', 13.6929, -89.2182)),
    ('Ethiopia', ('Addis Ababa', 9.03, 38.74)),
    ('Finland', ('Helsinki', 60.1695, 24.9354)),
    ('France', ('Paris', 48.8566, 2.3522)),
    ('Georgia', ('Tbilisi', 41.7151, 44.8271)),
    ('Germany', ('Berlin', 52.52, 13.405)),
    ('Ghana', ('Accra', 5.6037, -0.187)),
    ('Greece', ('Athens', 37.9838, 23.7275)),
    ('Guatemala', ('Guatemala City', 14.6349, -90.5069)),
    ('Honduras', ('Tegucigalpa', 14.0723, -87.1921)),
    ('India', ('New Delhi', 28.6139, 77.209)),
    ('Indonesia', ('Jakarta', -6.2088, 106.8456)),
    ('Iraq', ('Baghdad', 33.3152, 44.3661)),
    ('Ireland', ('Dublin', 53.3498, -6.2603)),
    ('Israel', ('Jerusalem', 31.7683, 35.2137)),
    ('Italy', ('Rome', 41.9028, 12.4964)),
    ('Japan', ('Tokyo', 35.6895, 139.6917)),
    ('Kenya', ('Nairobi', -1.2921, 36.8219)),
    ('Kuwait', ('Kuwait City', 29.3759, 47.9774)),
    ('Kyrgyzstan', ('Bishkek', 42.8746, 74.5698)),
    ('Libya', ('Tripoli', 32.8872, 13.1913)),
    ('Madagascar', ('Antananarivo', -18.8792, 47.5079)),
    ('Malaysia', ('Kuala Lumpur', 3.139, 101.6869)),
    ('Mali', ('Bamako', 12.6392, -8.0029)),
    ('Mexico', ('Mexico City', 19.4326, -99.1332)),
    ('Mongolia', ('Ulaanbaatar', 47.8864, 106.9057)),
    ('Morocco', ('Rabat', 34.0209, -6.8416)),
    ('Myanmar', ('Naypyidaw', 19.7633, 96.0785)),
    ('Nepal', ('Kathmandu', 27.7172, 85.324)),
    ('Netherlands', ('Amsterdam', 52.3676, 4.9041)),
    ('New Zealand', ('Wellington', -41.2865, 174.7762)),
    ('Niger', ('Niamey', 13.5128, 2.1127)),
    ('Nigeria', ('Abuja', 9.0765, 7.3986)),
    ('Norway', ('Oslo', 59.9139, 10.7522)),
    ('Oman', ('Muscat', 23.588, 58.3829)),
    ('Pakistan', ('Islamabad', 33.6844, 73.0479)),
    ('Panama', ('Panama City', 8.9824, -79.5199)),
    ('Paraguay', ('Asunción', -25.2637, -57.5759)),
    ('Peru', ('Lima', -12.0464, -77.0428)),
    ('Philippines', ('Manila', 14.5995, 120.9842)),
    ('Poland', ('Warsaw', 52.2297, 21.0122)),
    ('Portugal', ('Lisbon', 38.7169, -9.1399)),
    ('Qatar', ('Doha', 25.276987, 51.520008)),
    ('Romania', ('Bucharest', 44.4268, 26.1025)),
    ('Saudi Arabia', ('Riyadh', 24.7136, 46.6753)),
    ('Senegal', ('Dakar', 14.6928, -17.4467)),
    ('Serbia', ('Belgrade', 44.7866, 20.4489)),
    ('Singapore', ('Singapore', 1.3521, 103.8198)),
    ('Slovakia', ('Bratislava', 48.1486, 17.1077)),
    ('Somalia', ('Mogadishu', 2.0469, 45.3182)),
    ('South Africa', ('Pretoria', -25.7479, 28.2293)),
    ('Spain', ('Madrid', 40.4168, -3.7038)),
    ('Sri Lanka', ('Sri Jayawardenepura Kotte', 6.9271, 79.8612)),
    ('Sudan', ('Khartoum', 15.5007, 32.5599)),
    ('Switzerland', ('Bern', 46.9481, 7.4474)),
    ('Tajikistan', ('Dushanbe', 38.5598, 68.787)),
    ('Thailand', ('Bangkok', 13.7563, 100.5018)),
    ('Turkmenistan', ('Ashgabat', 37.9601, 58.3261)),
    ('Uganda', ('Kampala', 0.3476, 32.5825)),
    ('Ukraine', ('Kyiv', 50.4501, 30.5234)),
    ('United Arab Emirates', ('Abu Dhabi', 24.4539, 54.3773)),
    ('United Kingdom', ('London', 51.5074, -0.1278)),
    ('United States', ('Washington D.C.', 38.9072, -77.0369)),
    ('Uzbekistan', ('Tashkent', 41.2995, 69.2401)),
    ('Yemen', ("Sana'a", 15.3694, 44.191)),
    ('Zimbabwe', ('Harare', -17.8292, 31.0522))
]

def fetch_weather_data(country, city, lat, lon):
    try:
        station = Stations().nearby(lat, lon).fetch(1)
        if station.empty:
            print(f"No station found near {city}, {country}")
            return pd.DataFrame()
        data = Daily(station.index[0], start, end).fetch()
        if data.empty:
            print(f"No data available for {city}, {country}")
            return pd.DataFrame()
        data = data.reset_index()
        data['country'] = country
        data['city'] = city
        data = data[columns_to_keep + ['country', 'city']]
        data = data.rename(columns={'time': 'date'})
        return data
    except Exception as e:
        print(f"Error for {country}, {city}: {e}")
        return pd.DataFrame()

# Collect data
dfs = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_weather_data, country, city, lat, lon) for country, (city, lat, lon) in capital_coords]
    for future in futures:
        df = future.result()
        if not df.empty:
            dfs.append(df)

# Save combined data
if dfs:
    final_df = pd.concat(dfs, ignore_index=True)
    final_df.to_csv(os.path.join(output_dir, "weather_data_top100.csv"), index=False)
    print("✅ Combined weather data saved to 'weather_data/weather_data_top100.csv'")
else:
    print("⚠️ No data collected.")
