#!/usr/bin/env python3
"""Validate and normalize Saudi customer addresses using the TARA Locations API.

Fetches canonical city/region names from api.taraformula.com and builds a mapping
from messy Magento city names to their correct spellings.

Usage:
    python validate_addresses.py --fetch-cities              # Fetch canonical cities from API
    python validate_addresses.py --validate FILE.csv         # Validate cities in CSV
    python validate_addresses.py --validate FILE.csv --fix   # Validate + apply fixes to CSV

Environment:
    LOCATIONS_API_TOKEN=<Bearer token for api.taraformula.com>
"""

import argparse
import csv
import json
import os
import re
import sys
from collections import Counter

import requests
from dotenv import load_dotenv

API_BASE = "https://api.taraformula.com/api/locations/v1"
CITIES_CACHE = "data/saudi_cities.json"


def fetch_country_levels(token, iso2="SA"):
    """Fetch address levels for a country (regions, cities, districts)."""
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    resp = requests.post(
        f"{API_BASE}/country",
        json={"iso2": iso2, "detailed": True},
        headers=headers,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise ValueError(f"API error: {data.get('error', 'unknown')}")
    return data["items"][0] if data.get("items") else data


def fetch_address_data(token, level_id, parent_id=None):
    """Fetch all address data for a given level (e.g. all cities, all regions)."""
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {"address_level_id": level_id}
    if parent_id:
        payload["address_data_parent_id"] = parent_id

    resp = requests.post(
        f"{API_BASE}/data",
        json=payload,
        headers=headers,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise ValueError(f"API error: {data.get('error', 'unknown')}")
    return data.get("items", [])


def fetch_all_saudi_cities(token):
    """Fetch all Saudi regions, cities, and build a canonical name mapping."""
    print("Fetching Saudi Arabia address structure...")
    country = fetch_country_levels(token)
    levels = country.get("levels", [])
    print(f"  Address levels: {[l.get('name_en', l.get('name', '')) for l in levels]}")

    result = {"regions": [], "cities": [], "levels": levels}

    # Fetch each level's data
    for level in levels:
        level_id = level["id"]
        level_name = level.get("name_en", level.get("name", f"level_{level_id}"))
        print(f"\n  Fetching {level_name} (level {level_id})...")

        items = fetch_address_data(token, level_id)
        print(f"    Got {len(items)} items")

        for item in items:
            entry = {
                "id": item["id"],
                "name_en": item.get("name_en", ""),
                "name_ar": item.get("name_ar", ""),
                "name": item.get("name", ""),
            }
            if "Region" in level_name or level_id == levels[0]["id"]:
                result["regions"].append(entry)
            else:
                result["cities"].append(entry)

    return result


def build_city_lookup(cities_data):
    """Build a lookup dict: lowercase variant → canonical city name (English + Arabic).

    Creates entries for name_en, name_ar, and common misspellings.
    """
    lookup = {}

    for city in cities_data.get("cities", []):
        en = city.get("name_en", "").strip()
        ar = city.get("name_ar", "").strip()
        canonical = en or ar

        if not canonical:
            continue

        # Exact matches
        if en:
            lookup[en.lower()] = en
            # Without diacritics/special chars
            simple = re.sub(r"[''ʿʾ]", "", en).lower()
            if simple != en.lower():
                lookup[simple] = en
            # Without "Al " prefix
            if en.lower().startswith("al "):
                lookup[en[3:].lower()] = en
            # Without "Al-" prefix
            if en.lower().startswith("al-"):
                lookup[en[3:].lower()] = en

        if ar:
            lookup[ar] = en if en else ar

    # Also add regions
    for region in cities_data.get("regions", []):
        en = region.get("name_en", "").strip()
        ar = region.get("name_ar", "").strip()
        if en:
            lookup[en.lower()] = en
        if ar:
            lookup[ar] = en if en else ar

    return lookup


# Common misspellings and variants found in Magento data
_MANUAL_CITY_MAP = {
    # Case variants
    "riyadh": "Riyadh",
    "jeddah": "Jeddah",
    "dammam": "Dammam",
    "makkah": "Makkah",
    "medina": "Al Madinah Al Munawwarah",
    "madinah": "Al Madinah Al Munawwarah",
    "madina": "Al Madinah Al Munawwarah",
    "medinah": "Al Madinah Al Munawwarah",
    "tabuk": "Tabuk",
    "tabouk": "Tabuk",
    "khobar": "Al Khobar",
    "alkhobar": "Al Khobar",
    "al khobar": "Al Khobar",
    "alkobar": "Al Khobar",
    "al kohber": "Al Khobar",
    "al khubar": "Al Khobar",
    "akhobar": "Al Khobar",
    "jubail": "Al Jubail",
    "al jubail": "Al Jubail",
    "qatif": "Al Qatif",
    "al qatif": "Al Qatif",
    "alqatif": "Al Qatif",
    "qatef": "Al Qatif",
    "al qatief": "Al Qatif",
    "hufuf": "Al Hufuf",
    "al hufuf": "Al Hufuf",
    "alhufuf": "Al Hufuf",
    "hofuf": "Al Hufuf",
    "alhofuf": "Al Hufuf",
    "al hofuf": "Al Hufuf",
    "dhahran": "Dhahran",
    "dahran": "Dhahran",
    "aldhahran": "Dhahran",
    "al dhahran": "Dhahran",
    "mecca": "Makkah",
    "makkah al mukarramah": "Makkah",
    "makkeh": "Makkah",
    "makahh": "Makkah",
    "makkh": "Makkah",
    "taif": "At Taif",
    "at taif": "At Taif",
    "abha": "Abha",
    "yanbu": "Yanbu",
    "buraydah": "Buraydah",
    "buraidah": "Buraydah",
    "bureidah": "Buraydah",
    "najran": "Najran",
    "nejran": "Najran",
    "hail": "Hail",
    "sakaka": "Sakaka",
    "skaka": "Sakaka",
    "jazan": "Jazan",
    "jizan": "Jazan",
    "gizan": "Jazan",
    "arar": "Arar",
    "ar'ar": "Arar",
    "al ahsa": "Al Ahsa",
    "alahsa": "Al Ahsa",
    "alhasa": "Al Ahsa",
    "al hasa": "Al Ahsa",
    "alhassa": "Al Ahsa",
    "al hassa": "Al Ahsa",
    "ahsaa": "Al Ahsa",
    "alahsaa": "Al Ahsa",
    "alahssa": "Al Ahsa",
    "al baha": "Al Baha",
    "albaha": "Al Baha",
    "al kharj": "Al Kharj",
    "alkharj": "Al Kharj",
    "alkharaj": "Al Kharj",
    "al-kharj": "Al Kharj",
    "al-kahrj": "Al Kharj",
    "kharj": "Al Kharj",
    "hafar al batin": "Hafar Al Batin",
    "hafar al-batin": "Hafar Al Batin",
    "hafar albatin": "Hafar Al Batin",
    "hafer albatin": "Hafar Al Batin",
    "hafer albaten": "Hafar Al Batin",
    "hafar albateen": "Hafar Al Batin",
    "hafr al-batin": "Hafar Al Batin",
    "khamis mushayt": "Khamis Mushayt",
    "khamis mushait": "Khamis Mushayt",
    "khamis mushit": "Khamis Mushayt",
    "khamis moushait": "Khamis Mushayt",
    "khamis mutair": "Khamis Mushayt",
    "kamis mashet": "Khamis Mushayt",
    "kamis mushit": "Khamis Mushayt",
    "al mubarraz": "Al Mubarraz",
    "almubarraz": "Al Mubarraz",
    "unayzah": "Unayzah",
    "unizah": "Unayzah",
    "unaizah": "Unayzah",
    "onizah": "Unayzah",
    "sayhat": "Sayhat",
    "saihat": "Sayhat",
    "sihat": "Sayhat",
    "al madinah al munawwarah": "Al Madinah Al Munawwarah",
    "almadinah": "Al Madinah Al Munawwarah",
    "almadinh": "Al Madinah Al Munawwarah",
    "al-madinah": "Al Madinah Al Munawwarah",
    "rabigh": "Rabigh",
    "bishah": "Bishah",
    "bisha": "Bishah",
    "safwa": "Safwa",
    "tarut": "Tarut",
    "taroot": "Tarut",
    "sabya": "Sabya",
    "al qunfidhah": "Al Qunfidhah",
    "al qunfudhah": "Al Qunfidhah",
    "alqunfdah": "Al Qunfidhah",
    "zulfi": "Az Zulfi",
    "az zulfi": "Az Zulfi",
    "zelfi": "Az Zulfi",
    "ras tanura": "Ras Tannurah",
    "ras tannurah": "Ras Tannurah",
    "ras tanourah": "Ras Tannurah",
    "rastanura": "Ras Tannurah",
    "al khafji": "Al Khafji",
    "alkhfji": "Al Khafji",
    "muhayil": "Muhayil",
    "ad duwadimi": "Ad Duwadimi",
    "aldwadmi": "Ad Duwadimi",
    "dawadmi": "Ad Duwadimi",
    "dawmat al jandal": "Dawmat Al Jandal",
    "shaqra": "Shaqra",
    "duba": "Duba",
    "thuwal": "Thuwal",
    "al ula": "Al Ula",
    "alula": "Al Ula",
    "turaif": "Turaif",
    "afif": "Afif",
    "badr": "Badr",
    "khaybar": "Khaybar",
    "abu arish": "Abu Arish",
    "aboarish": "Abu Arish",
    "samtah": "Samtah",
    "haql": "Haql",
    "ar rass": "Ar Rass",
    "al-rass": "Ar Rass",
    "al bukayriyah": "Al Bukayriyah",
    "albukayriyah": "Al Bukayriyah",
    "biljurashi": "Biljurashi",
    "namas": "Namas",
    "umluj": "Umluj",
    "al wajh": "Al Wajh",
    "abqaiq": "Buqayq",
    "baqiq": "Buqayq",
    "buqayq": "Buqayq",
    "al lith": "Al Lith",
    "sharurah": "Sharurah",
    "shrorah": "Sharurah",
    "rafha": "Rafha",
    "al midhnab": "Al Midhnab",
    "turbah": "Turbah",
    "al jumum": "Al Jumum",
    "ahad rufaidah": "Ahad Rifaydah",
    "ahad rifaydah": "Ahad Rifaydah",
    "al majma'ah": "Al Majma'ah",
    "almajmaah": "Al Majma'ah",
    "majmaah": "Al Majma'ah",
    "hawtat bani tamim": "Hawtat Bani Tamim",
    "wadi ad dawasir": "Wadi Ad Dawasir",
    "wadi aldwasser": "Wadi Ad Dawasir",
    "al artawiyah": "Al Artawiyah",
    "alartawiyah": "Al Artawiyah",
    "khulays": "Khulays",
    "hawtat sudair": "Hawtat Sudair",
    "hautat sudair": "Hawtat Sudair",
    # Additional misspellings from Magento data
    "jeedah": "Jeddah",
    "jedda": "Jeddah",
    "jaddah": "Jeddah",
    "geedah": "Jeddah",
    "jedfah": "Jeddah",
    "royadh": "Riyadh",
    "riyad": "Riyadh",
    "riadh": "Riyadh",
    "riydh": "Riyadh",
    "riydah": "Riyadh",
    "riaydh": "Riyadh",
    "tiyadh": "Riyadh",
    "alriyadh": "Riyadh",
    "al riyadh": "Riyadh",
    "cammam": "Dammam",
    "ad dammam": "Dammam",
    "al dammam": "Dammam",
    "meco": "Makkah",
    "ad dir'iyah": "Ad Dir'iyah",
    "ad diriyah": "Ad Dir'iyah",
    "qassim": "Buraydah",
    "alqassim": "Buraydah",
    "al qassim": "Buraydah",
    "qaseem": "Buraydah",
    "al majmaah": "Al Majma'ah",
    "shaqra'": "Shaqra",
    "alghat": "Al Ghat",
    "laban": "Riyadh",
    "al malqa": "Riyadh",
    "al rawdah": "Riyadh",
    "ar rabwa": "Riyadh",
    "al nuzha": "Riyadh",
    "irqah": "Riyadh",
    "'irqah": "Riyadh",
    "jarir": "Riyadh",
    "al rajhiyah": "Riyadh",
    "an nakhil": "Al Madinah Al Munawwarah",
    "al muhammadiyah": "Al Madinah Al Munawwarah",
    "al aziziyah": "Makkah",
    "al hazim": "Hail",
    "al salam": "Riyadh",
    "al uyun": "Al Ahsa",
    "al jish": "Al Qatif",
    "al jishshah": "Al Qatif",
    "rayda": "Abha",
    "rumah": "Riyadh",
    "yanbu al sinaiyah": "Yanbu",
    "yanbu indestrial": "Yanbu",
    "yanbu industrial": "Yanbu",
    "industrial yanbu": "Yanbu",
    "alhsaa": "Al Ahsa",
    "ar riyadh": "Riyadh",
    "almadinah almunawwarah": "Al Madinah Al Munawwarah",
    "amadina almnwarah": "Al Madinah Al Munawwarah",
    "almadina almnwra": "Al Madinah Al Munawwarah",
    "almadinh almnorah": "Al Madinah Al Munawwarah",
    "abha almansk": "Abha",
    "anak": "Anak",
    "olyaa": "Riyadh",
    "al 'ulya": "Riyadh",
    "al 'ulayya": "Riyadh",
    "al khadra": "Riyadh",
    "al fayha": "Riyadh",
    "an nuzhah": "Riyadh",
    "as sulaimaniyah": "Riyadh",
    "alyasmeen": "Riyadh",
    "arrabi": "Riyadh",
    "al hazm": "Hail",
    "al hazmi": "Hail",
    "ath thuqbah": "Al Khobar",
    "al doha aljnobiah": "Dhahran",
    "aldoha aljanobiah": "Dhahran",
    "darin": "Al Qatif",
    "al qurayyat": "Al Qurayyat",
    "alhufuf": "Al Hufuf",
    "al hufuf": "Al Hufuf",
    "baysh": "Jazan",
    "ad darb": "Jazan",
    "an nadhim": "Riyadh",
    "ath thumamy": "Riyadh",
    "ath thumamah": "Riyadh",
    "jubail industrial city": "Al Jubail",
    "jubail industrial": "Al Jubail",
    "jubail indurstial city": "Al Jubail",
    "ar rawdah": "Riyadh",
    "as sufarat": "Riyadh",
    "madain as salih": "Al Ula",
    "al atawlah": "Al Baha",
    "sajir": "Riyadh",
    "al khutamah": "Abha",
    "altaraf": "Al Ahsa",
    "hafar albaten": "Hafar Al Batin",
    "dhahrab": "Dhahran",
    "asser": "Asir",
    "aseer": "Asir",
    "almalaz": "Riyadh",
    "al-hasa": "Al Ahsa",
    "alhasa-hufuf": "Al Hufuf",
    "al omran": "Al Ahsa",
    "al_omran": "Al Ahsa",
    "thoqba": "Al Khobar",
    "rakah janubia": "Al Khobar",
    "alshati al garpi": "Jeddah",
    "aldar albiydah": "Riyadh",
    "dhiba": "Duba",
    "ushaqier": "Shaqra",
    "ushaiqer": "Shaqra",
    "alwajh": "Al Wajh",
    "almadinah almunwwrah": "Al Madinah Al Munawwarah",
    "yanbu albahar": "Yanbu",
    "yanbu royal commission": "Yanbu",
    "hellat muhaish": "Al Ahsa",
    "mehd": "Al Madinah Al Munawwarah",
    "damad": "Jazan",
    "illisha": "Al Ahsa",
    "al silimiyah": "Riyadh",
    "as silimiyah": "Riyadh",
    "hota bni tammim": "Hawtat Bani Tamim",
    "herra": "Makkah",
    "banban": "Riyadh",
    "al hamr": "Abha",
    "al hamra": "Riyadh",
    "al khaldiyah": "Jeddah",
    "al khalidiyah": "Jeddah",
    "al-baha": "Al Baha",
    "alaziziya": "Makkah",
    "medinh": "Al Madinah Al Munawwarah",
    "medine": "Al Madinah Al Munawwarah",
    "meddinah": "Al Madinah Al Munawwarah",
    "almandine": "Al Madinah Al Munawwarah",
    "almadena": "Al Madinah Al Munawwarah",
    "tarout": "Tarut",
    "buraydh": "Buraydah",
    "a bah": "Abha",
    "alharameen": "Al Madinah Al Munawwarah",
    "alqasab": "Shaqra",
    "alalhssa": "Al Ahsa",
    "qunfudah": "Al Qunfidhah",
    "alriyad": "Riyadh",
    "alnaja": "Riyadh",
    "alhafuf": "Al Hufuf",
    "al hawiyah": "At Taif",
    "yanbu'al-baḥr": "Yanbu",
    "yanbu' an nakhil": "Yanbu",
    "king khalid military city": "Hafar Al Batin",
    "algassem buryda": "Buraydah",
    "exit14": "Riyadh",
    "qara": "Al Ahsa",
    "sharma": "Tabuk",
    "raghdan": "Al Baha",
    "radwa": "Yanbu",
    "marat": "Al Qassim",
    "madrakah": "Makkah",
    "dhabhah": "Tabuk",
    "king abdullah economic city": "King Abdullah Economic City",
    "at taraf": "Al Ahsa",
    "al ghazalah": "Hail",
    "ar ruwaydah": "Al Qassim",
    "al hait": "Hail",
    "as sulayyil": "As Sulayyil",
    "uqlat as suqur": "Al Qassim",
    "al burud": "Al Qassim",
    "al ahsa umran": "Al Ahsa",
    "ar rawdah exit 11": "Riyadh",
    "industrial jubail": "Al Jubail",
    "sharqiyah": "Al Jubail",
    # Arabic common variants
    "جدة": "Jeddah",
    "جده": "Jeddah",
    "الرياض": "Riyadh",
    "رياض": "Riyadh",
    "الدمام": "Dammam",
    "مكة المكرمة": "Makkah",
    "مكة": "Makkah",
    "مكه": "Makkah",
    "مكه المكرمه": "Makkah",
    "المدينة المنورة": "Al Madinah Al Munawwarah",
    "المدينه المنوره": "Al Madinah Al Munawwarah",
    "المدينه المنورة": "Al Madinah Al Munawwarah",
    "المدينةالمنوره": "Al Madinah Al Munawwarah",
    "المدينه": "Al Madinah Al Munawwarah",
    "المدينة": "Al Madinah Al Munawwarah",
    "الطائف": "At Taif",
    "الطايف": "At Taif",
    "الخبر": "Al Khobar",
    "تبوك": "Tabuk",
    "الجبيل": "Al Jubail",
    "الجبيل الصناعية": "Al Jubail",
    "الجبيل الصناعيه": "Al Jubail",
    "القطيف": "Al Qatif",
    "الظهران": "Dhahran",
    "بريدة": "Buraydah",
    "بريده": "Buraydah",
    "المبرز": "Al Mubarraz",
    "ابها": "Abha",
    "حفر الباطن": "Hafar Al Batin",
    "حائل": "Hail",
    "ينبع": "Yanbu",
    "ينبع الصناعية": "Yanbu",
    "نجران": "Najran",
    "خميس مشيط": "Khamis Mushayt",
    "خميس مسبط": "Khamis Mushayt",
    "الخرج": "Al Kharj",
    "سكاكا": "Sakaka",
    "جازان": "Jazan",
    "جيزان": "Jazan",
    "الهفوف": "Al Hufuf",
    "صفوى": "Safwa",
    "سيهات": "Sayhat",
    "عنيزة": "Unayzah",
    "عنيزه": "Unayzah",
    "عرعر": "Arar",
    "الاحساء": "Al Ahsa",
    "القنفذة": "Al Qunfidhah",
    "رابغ": "Rabigh",
    "الباحه": "Al Baha",
    "بيشة": "Bishah",
    "الرس": "Ar Rass",
    "تاروت": "Tarut",
    "راس تنورة": "Ras Tannurah",
    "راس تنوره": "Ras Tannurah",
    "الخفجي": "Al Khafji",
    "بقيق": "Buqayq",
    "البكيرية": "Al Bukayriyah",
    "البكيريه": "Al Bukayriyah",
    "الوجه": "Al Wajh",
    "السليل": "As Sulayyil",
    "شقراء": "Shaqra",
    "العلا": "Al Ula",
    "المزاحمية": "Al Muzahimiyah",
    "بلجرشي": "Biljurashi",
    "تيماء": "Tayma",
    "وادي الدواسر": "Wadi Ad Dawasir",
    "الدوادمي": "Ad Duwadimi",
    "المجمعة": "Al Majma'ah",
    "ضرما": "Durma",
    "القيصومة": "Al Qaysumah",
    "القريات": "Al Qurayyat",
    "حوطة بني تميم": "Hawtat Bani Tamim",
    "حوطة سدير": "Hawtat Sudair",
    "رفحاء": "Rafha",
    "ابو عريش": "Abu Arish",
    "احد رفيده": "Ahad Rifaydah",
    "خيبر": "Khaybar",
    "املج": "Umluj",
    "النماص": "Namas",
    "حقل": "Haql",
    "ضبا": "Duba",
    "الغاط": "Al Ghat",
    "طريف": "Turaif",
    "عفيف": "Afif",
    "فرسان": "Farasan",
    "القويعية": "Al Quway'iyah",
    "صبيا": "Sabya",
    "العيون": "Al Uyun",
    "المظيلف": "Al Mudhaylif",
    "العارضة": "Al Aridah",
    "الجشة": "Al Jishshah",
    "بلقرن": "Balqarn",
    "مدينة الملك خالد العسكرية": "King Khalid Military City",
    "هروب": "Hurub",
}


# Garbage values that are not real cities
_GARBAGE_CITIES = {
    "saudi arabia", "saudi", "ksa", "saudia", "saudiarabia",
    "eastern province", "eastern", "eastarn",
    "city", "home", "j", "kl", "2", "66557", "23422",
    "saudi / jeddah", "saudi arabia , qassim - almidhab",
}


def normalize_city(raw_city):
    """Normalize a city name using the manual mapping.

    Returns (normalized_city, was_changed).
    """
    if not raw_city:
        return "", False

    clean = raw_city.strip()

    # Replace non-breaking spaces with regular spaces
    clean = clean.replace("\xa0", " ")

    # Decode HTML entities
    clean = clean.replace("&#039;", "'").replace("&amp;", "&")

    # Strip junk suffixes (zip codes, district names appended)
    # e.g. "Dhahran 34248", "Makkah 24351 - 7731", "Dammam 6606"
    clean = re.sub(r"\s+\d{4,}.*$", "", clean)

    # Strip "Saudi Arabia" / "KSA" suffix
    clean = re.sub(r"[,/]\s*Saudi Arabia\s*$", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\s+Saudi Arabia\s*$", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r",?\s*SA\s*$", "", clean)

    # Strip district/neighborhood suffixes after main city
    # e.g. "Riyadh, alnarjas" → "Riyadh", "Riyadh Gurtupa" → complex
    if ", " in clean:
        main_part = clean.split(",")[0].strip()
        if main_part.lower() in _MANUAL_CITY_MAP or main_part in (
            "Riyadh", "Jeddah", "Dammam", "Makkah", "Dhahran",
        ):
            clean = main_part

    # Strip "/district" suffixes: "Riyadh/Alkharj" → "Riyadh"
    # Also try last part: "Ksa / jubail" → "jubail"
    if "/" in clean:
        parts = [p.strip() for p in clean.split("/")]
        for part in parts:
            if part.lower() in _MANUAL_CITY_MAP:
                clean = part
                break

    # Handle "Region - City" or "City - District": "Asir- abha" → "abha"
    if "-" in clean and " - " not in clean:
        parts = [p.strip() for p in clean.split("-")]
        for part in reversed(parts):
            if part.lower() in _MANUAL_CITY_MAP:
                clean = part
                break

    # Check garbage
    if clean.lower().strip() in _GARBAGE_CITIES:
        return "", True

    # Check manual map (exact, case-insensitive)
    key = clean.lower().strip()
    if key in _MANUAL_CITY_MAP:
        mapped = _MANUAL_CITY_MAP[key]
        return mapped, mapped != clean

    # Check Arabic (exact match, case-sensitive)
    if clean in _MANUAL_CITY_MAP:
        mapped = _MANUAL_CITY_MAP[clean]
        return mapped, mapped != clean

    # Strip leading numbers/addresses: "139/ jeddah" → "jeddah"
    clean = re.sub(r"^\d+\s*/?\s*", "", clean).strip()
    # Strip LTR/RTL marks
    clean = re.sub(r"[\u200e\u200f\u202a-\u202e]", "", clean).strip()

    # Re-check after cleanup
    key = clean.lower().strip()
    if key in _MANUAL_CITY_MAP:
        return _MANUAL_CITY_MAP[key], True

    # Try "City District" pattern: "Dammam Alhamra" → "Dammam"
    # "Riyadh Gurtupa" → "Riyadh", "Taif Alwesam" → "At Taif"
    first_word = clean.split()[0] if clean else ""
    if first_word.lower() in _MANUAL_CITY_MAP:
        return _MANUAL_CITY_MAP[first_word.lower()], True

    # Try "X - Y" pattern: "Thoqba - Khobar" → take last part
    if " - " in clean:
        parts = [p.strip() for p in clean.split(" - ")]
        for part in reversed(parts):
            if part.lower() in _MANUAL_CITY_MAP:
                return _MANUAL_CITY_MAP[part.lower()], True

    # Try stripping common suffixes: "Jubail Industrial City" → "Jubail"
    for suffix in [" industrial city", " industrial", " city", " region"]:
        if suffix in clean.lower():
            prefix = clean[:clean.lower().index(suffix)].strip()
            if prefix.lower() in _MANUAL_CITY_MAP:
                return _MANUAL_CITY_MAP[prefix.lower()], True

    return clean, False


def validate_csv_cities(csv_path):
    """Analyze cities in a customer CSV and report normalization results."""
    raw_cities = Counter()
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            country = (row.get("Country") or "").strip()
            if "Saudi" not in country and "saudi" not in country:
                continue
            city = (row.get("City") or "").strip()
            if city:
                raw_cities[city] += 1

    print(f"\nTotal unique cities: {len(raw_cities)}")
    print(f"Total addresses: {sum(raw_cities.values())}")

    normalized = 0
    cleared = 0
    unchanged = 0
    unmatched = Counter()

    for city, count in raw_cities.most_common():
        norm, changed = normalize_city(city)
        if not norm:
            cleared += count
            print(f"    CLEAR: {city!r} ({count})")
        elif changed:
            normalized += count
        else:
            # Check if the raw value itself is clean
            has_nbsp = "\xa0" in city
            has_entity = "&#" in city
            if has_nbsp or has_entity:
                # It will be cleaned by normalize_city, count as normalized
                normalized += count
            else:
                # If the raw value differs from the normalized (even if normalize
                # returned changed=False due to suffix stripping etc), count it
                if norm != city:
                    normalized += count
                else:
                    unchanged += count
                    # Check if it looks suspicious (not in our mapping and not Arabic)
                    if not any("\u0600" <= c <= "\u06FF" for c in city):
                        if city.lower() not in _MANUAL_CITY_MAP:
                            unmatched[city] += count

    print(f"\nResults:")
    print(f"  Already correct: {unchanged}")
    print(f"  Normalized:      {normalized}")
    print(f"  Cleared (junk):  {cleared}")
    print(f"  Unmatched:       {sum(unmatched.values())} ({len(unmatched)} unique)")

    if unmatched:
        print(f"\nUnmatched cities (top 50):")
        for city, count in unmatched.most_common(50):
            norm, _ = normalize_city(city)
            print(f"  {count:4d}  {city!r} → {norm!r}")

    return unmatched


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Validate Saudi customer addresses")
    parser.add_argument("--fetch-cities", action="store_true",
                        help="Fetch canonical cities from Locations API")
    parser.add_argument("--validate", metavar="CSV",
                        help="Validate cities in a customer CSV")
    parser.add_argument("--api-token", help="Locations API Bearer token")
    parser.add_argument("--cross-validate", metavar="CSV",
                        help="Fetch cities from API and cross-validate CSV against them")
    args = parser.parse_args()

    token = args.api_token or os.environ.get("LOCATIONS_API_TOKEN")

    if args.fetch_cities:
        if not token:
            print("ERROR: Set LOCATIONS_API_TOKEN or use --api-token")
            sys.exit(1)

        cities_data = fetch_all_saudi_cities(token)
        os.makedirs("data", exist_ok=True)
        with open(CITIES_CACHE, "w", encoding="utf-8") as f:
            json.dump(cities_data, f, ensure_ascii=False, indent=2)
        print(f"\nSaved {len(cities_data['cities'])} cities to {CITIES_CACHE}")

        # Show what we got
        lookup = build_city_lookup(cities_data)
        print(f"Built lookup with {len(lookup)} entries")

    if args.validate:
        validate_csv_cities(args.validate)

    if args.cross_validate:
        if not token:
            print("ERROR: Set LOCATIONS_API_TOKEN or use --api-token")
            sys.exit(1)

        # Step 1: Fetch canonical cities from API
        print("=== Step 1: Fetching canonical cities from API ===")
        cities_data = fetch_all_saudi_cities(token)
        api_lookup = build_city_lookup(cities_data)
        print(f"  API has {len(cities_data.get('cities', []))} cities")

        # Step 2: Load and normalize CSV cities
        print("\n=== Step 2: Normalizing CSV cities ===")
        raw_cities = Counter()
        with open(args.cross_validate, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                country = (row.get("Country") or "").strip()
                if "Saudi" not in country and "saudi" not in country:
                    continue
                city = (row.get("City") or "").strip()
                if city:
                    raw_cities[city] += 1

        # Step 3: Cross-validate normalized names against API
        print("\n=== Step 3: Cross-validating against API ===")
        matched = 0
        api_unmatched = Counter()
        for city, count in raw_cities.most_common():
            norm, _ = normalize_city(city)
            if not norm:
                continue  # cleared garbage

            # Check if the normalized name exists in API
            if norm.lower() in api_lookup:
                api_canonical = api_lookup[norm.lower()]
                if api_canonical != norm:
                    print(f"  API SPELLING: {norm!r} → {api_canonical!r} ({count})")
                matched += count
            elif any("\u0600" <= c <= "\u06FF" for c in norm):
                # Arabic — check Arabic keys
                if norm in api_lookup:
                    matched += count
                else:
                    api_unmatched[norm] += count
            else:
                api_unmatched[norm] += count

        print(f"\n  Matched in API:   {matched}")
        print(f"  Not in API:       {sum(api_unmatched.values())} ({len(api_unmatched)} unique)")
        if api_unmatched:
            print(f"\n  Cities not found in API (top 30):")
            for city, count in api_unmatched.most_common(30):
                print(f"    {count:4d}  {city}")

    if not args.fetch_cities and not args.validate and not args.cross_validate:
        parser.print_help()


if __name__ == "__main__":
    main()
