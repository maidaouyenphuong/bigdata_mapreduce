#%%
import csv
from pymongo import MongoClient
from datetime import datetime, date
from typing import Dict, Tuple, Set, List, Any

# Step 1
# Connect to MongoDB
local_connection = MongoClient("mongodb://localhost:27017")
db = local_connection["Assignment_1"]
collection = db["AUS_weather"]

# Drop old collection to avoid duplicates
collection.drop()

# Open CSV and insert rows one by one
with open("data.csv", newline="", encoding="utf-8") as csvfile:
    # creates a dict for each row {col_name: value}
    reader = csv.DictReader(csvfile)  
    for row in reader:
        # Insert each row into MongoDB
        collection.insert_one(row)

#%%
# Step 2:
# Convert a value to Python date, expecting 'YYYY-MM-DD' 
def to_date(d: Any) -> date:
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    s = str(d).strip()
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        raise ValueError

# Generate a set of all valid dates in a given year to detect missing records
def is_leap(year: int) -> bool:
    # Leap year rule: divisible by 4 but not 100, unless also divisible by 400
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

def expected_day_set(year: int) -> Set[date]:
    # Days in each month for a normal year
    month_days = [31, 28, 31, 30, 31, 30,
                  31, 31, 30, 31, 30, 31]

    # Adjust February for leap year
    if is_leap(year):
        month_days[1] = 29

    # Create an empty set to hold all date objects
    days: Set[date] = set()

    month = 1
    for num_days in month_days:
        for day in range(1, num_days + 1):
            days.add(date(year, month, day))
        month += 1   
    return days

# Take only needed fields (Location, Date, MinTemp, MaxTemp, Rainfall, RainToday)
projection = {
    "_id": 0,
    "Location": 1,
    "Date": 1,
    "MinTemp": 1,
    "MaxTemp": 1,
    "Rainfall": 1,
    "RainToday": 1,
}

# Create dictionaries for tracking data
days_by_key: Dict[Tuple[str, int], Set[date]] = {}
docs_by_key: Dict[Tuple[str, int], List[dict]] = {}

# Go through every weather record one by one, and group rows by (Location, Year):
cursor = collection.find({}, projection)

for doc in cursor:
    try:
        loc = str(doc.get("Location", "")).strip()
        d = to_date(doc.get("Date"))
        yr = d.year
        key = (loc, yr)
    except Exception:
        continue

    if key not in days_by_key:
        days_by_key[key] = set()
        docs_by_key[key] = []

    days_by_key[key].add(d)
    docs_by_key[key].append(doc)

# Determine which (Location, Year) are COMPLETE
complete_keys: Set[Tuple[str, int]] = set()
for key, seen_days in days_by_key.items():
    yr = key[1]
    if seen_days == expected_day_set(yr):  # strict: must match the full calendar
        complete_keys.add(key)

# Format helpers
def fmt_date_iso(dval: Any) -> str:
    return to_date(dval).strftime("%Y-%m-%d")

def safe(v: Any) -> str:
    return "" if v is None else str(v)

# Write to file 
# No need to run in cmd
# I run in VSCode and it will automatically create the file and write output into the file
lines_written = 0
with open("observations.txt", "w", encoding="utf-8") as f:
    for key in sorted(complete_keys, key=lambda k: (k[0], k[1])):
        docs = docs_by_key[key]
        for r in docs:
            line = ",".join([
                safe(str(r.get("Location", "")).strip()),
                fmt_date_iso(r.get("Date")),
                safe(r.get("MinTemp")),
                safe(r.get("MaxTemp")),
                safe(r.get("Rainfall")),
                safe(r.get("RainToday")),
            ])
            f.write(line + "\n")
            lines_written += 1

#%%
