# Balad Places Scraper

A Python-based scraper for extracting place information (such as florists, restaurants, and other businesses) from the [Balad](https://balad.ir) API and saving the data into an Excel file.

## 📌 Features
- Fetches places' details like:
  - Name
  - Address
  - Phone number
  - Instagram handle (if available)
  - Website (if available)
- Saves data into a structured `.xlsx` file.
- Can be customized for any category and city in Iran.

## 🛠 Requirements
- Python 3.x
- Requests
- openpyxl

## 📦 Installation

```bash
git clone https://github.com/YourUsername/balad-places-scraper.git
cd balad-places-scraper
pip install -r requirements.txt
