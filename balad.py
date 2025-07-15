import aiohttp
import asyncio
import pandas as pd

BASE_URL = "https://search.raah.ir/v4/placeslist/cat/"
DETAIL_URL = "https://poi.raah.ir/web/v4/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://balad.ir",
    "Device-Id": "07f86946-4ea2-4c9c-807a-fed76105c434"
}

def show_banner():
    print("="*50)
    print("   🌸 Florist Scraper | By Shakiba 🌸")
    print("       Collecting florists data...")
    print("="*50)

async def fetch_tokens(session, page_num, city):
    params = {"region": city, "name": "florist", "page": page_num}
    async with session.get(BASE_URL, headers=HEADERS, params=params) as response:
        if response.status != 200:
            print(f"❌ Error on page {page_num}: {response.status}")
            return None
        data = await response.json()
        schema_items = data.get("seo_details", {}).get("schemas", [])
        if isinstance(schema_items, list) and len(schema_items) > 1:
            florists = schema_items[1]
            return [item.get("identifier") for item in florists if item.get("identifier")]
        return []

async def fetch_details(session, token, limiter, city_fa):
    async with limiter:
        async with session.get(f"{DETAIL_URL}{token}", headers=HEADERS) as response:
            if response.status != 200:
                return None
            data = await response.json()
            name = data.get("name", "")
            address = ""
            phone = ""
            instagram = ""
            whatsapp = ""
            telegram = ""
            eitaa = ""
            website = ""

            for field in data.get("fields", []):
                icon = field.get("icon")
                value = field.get("value", "")
                text = field.get("text", "")

                if icon == "gps":
                    address = value
                elif icon == "phone":
                    phone = text
                elif icon == "instagram":
                    instagram = value.replace("https://instagram.com/", "").replace("https://www.instagram.com/", "")
                elif icon == "whatsapp":
                    whatsapp = text
                elif icon == "telegram":
                    telegram = text
                elif icon == "eitaa":
                    eitaa = text
                elif icon == "website":
                    website = value

            return {
                "استان / شهر": city_fa,
                "نام گل‌فروشی": name,
                "آدرس": address,
                "شماره": phone,
                "اینستاگرام": instagram,
                "واتساپ": whatsapp,
                "تلگرام": telegram,
                "ایتا": eitaa,
                "وبسایت": website
            }

async def run_scraper(city_code, city_fa, output_file):
    show_banner()
    limiter = asyncio.Semaphore(10)
    tokens = []
    async with aiohttp.ClientSession() as session:
        page = 1
        while True:
            ids = await fetch_tokens(session, page, city_code)
            if ids is None or len(ids) == 0:
                break
            tokens.extend(ids)
            print(f"Page {page} ✔️  —  {len(ids)} IDs found")
            page += 1

        print(f"\n🎯 Total identifiers collected: {len(tokens)}\n")

        tasks = [fetch_details(session, token, limiter, city_fa) for token in tokens]
        results = await asyncio.gather(*tasks)

        final_data = [item for item in results if item]
        df = pd.DataFrame(final_data)

        df.to_excel(output_file, index=False)
        print(f"✅ Data saved to {output_file}")

# اجرای نهایی
city_code = "city-kashan"
city_fa = "اصفهان-کاشان"
output_file = "kashan_florists_final.xlsx"

asyncio.run(run_scraper(city_code, city_fa, output_file))
