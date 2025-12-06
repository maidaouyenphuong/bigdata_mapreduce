from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
from pathlib import Path

INPUT_NAME = "observations.txt"
OUTPUT_NAME = "output1_2.txt"

#Parse YYYY-MM-DD (or strings that start with it) into ISO 'YYYY-MM-DD'.
def to_date_iso(text: str) -> str:
    try:
        return datetime.strptime(str(text).strip()[:10], "%Y-%m-%d").date().isoformat()
    except Exception:
        return ""

class MRFormatByLocation(MRJob):
    # MAP
    def mapper(self, _, line: str):
        # Split CSV row into fields
        parts = [c.strip() for c in line.strip().split(",")]

        loc, date_text, min_temp, max_temp, rainfall, rain_today = parts

        # Parse/normalize date
        date_iso = to_date_iso(date_text)
        if not loc or not date_iso:
            return

        # Build the value string (without quotes; we’ll add quotes when writing)
        value_str = ",".join([date_iso, min_temp, max_temp, rainfall, rain_today])

        # Emit: key = location, value = (date_iso, value_str)
        yield loc, (date_iso, value_str)

    # REDUCE
    def reducer(self, loc: str, values):
        # Sort rows by date ascending
        # t = (date_iso, value_str)
        rows = sorted(values, key=lambda t: t[0])  
        for date_iso, value_str in rows:
            # Emit (key, value) 
            yield loc, value_str

# Write to file 
# No need to run in cmd
# I run in VSCode and it will automatically create the file and write output into the file
if __name__ == "__main__":
    job = MRFormatByLocation(args=[INPUT_NAME])
    with job.make_runner() as runner:
        runner.run()
        out_path = Path(OUTPUT_NAME)
        # newline="" avoids double-blank-lines on Windows
        with out_path.open("w", encoding="utf-8", newline="") as fout:
            # parse_output gives you (key, value) objects (already decoded)
            for pair in job.parse_output(runner.cat_output()):
                location = pair[0]
                value_str = pair[1]
                # Final format exactly as requested:
                # "Location" "Date,MinTemp,MaxTemp,Rainfall,RainToday"
                fout.write(f"\"{location}\" \"{value_str}\"\n")
