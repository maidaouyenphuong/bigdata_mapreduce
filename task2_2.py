from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
from pathlib import Path

INPUT_NAME = "observations.txt"
OUTPUT_NAME = "output2_2.txt"

# Parse 'YYYY-MM-DD' safely -> date or None
def to_date_yyyy_mm_dd(s: str):
    try:
        return datetime.strptime(str(s).strip()[:10], "%Y-%m-%d").date()
    except Exception:
        return None

# Parse float safely -> float or None
def to_float_safe(x: str):
    try:
        return float(str(x).strip())
    except Exception:
        return None

class MRTop3ByAvgAnnualRainYes(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_sum_yearly_yes_rain,
                reducer=self.reducer_sum_yearly_yes_rain,
            ),
            MRStep(
                reducer=self.reducer_top3_from_yearly_totals,
            ),
        ]

    # MAP: emit ((Location, Year), Rainfall) but only when RainToday == "Yes"
    def mapper_sum_yearly_yes_rain(self, _, line: str):
        parts = [p.strip() for p in line.strip().split(",")]

        #extract values
        loc = parts[0]
        d = to_date_yyyy_mm_dd(parts[1])
        rainfall = to_float_safe(parts[4])
        rain_today = parts[5].lower()

        # must have valid date & rainfall number
        if d is None or rainfall is None:
            return
        
        if rain_today == "yes":
            # key: (location, year); value: rainfall for that day
            yield (loc, d.year), rainfall

    # REDUCER 1: sum rainfall per (Location, Year)
    def reducer_sum_yearly_yes_rain(self, key, values):
        loc, _year = key
        yearly_total = sum(values)
        # forward as (None, (loc, yearly_total)) so next reducer gets all
        yield None, (loc, yearly_total)

    # REDUCER 2: average the yearly totals per location, pick Top-3 by avg desc
    def reducer_top3_from_yearly_totals(self, _none, loc_yearly_totals_iter):
        sums = {}
        counts = {}

        for loc, yearly_total in loc_yearly_totals_iter:
            sums[loc] = sums.get(loc, 0.0) + yearly_total
            counts[loc] = counts.get(loc, 0) + 1

        # compute averages
        avgs = [(loc, (sums[loc] / counts[loc])) for loc in sums if counts[loc] > 0]
        # sort by avg (desc), then location (asc)
        avgs.sort(key=lambda x: (-x[1], x[0]))

        # emit Top-3
        for loc, avg in avgs[:3]:
            yield loc, f"{avg:.2f}"

# Write to file 
# No need to run in cmd
# I run in VSCode and it will automatically create the file and write output into the file
def run_job_to_file(job_cls, in_name, out_name):
    in_path = str(Path.cwd() / in_name)
    out_path = Path.cwd() / out_name

    job = job_cls(args=["--no-conf", in_path])

    with job.make_runner() as runner:
        runner.run()
        with out_path.open("w", encoding="utf-8", newline="") as fout:
            for loc, avg_str in job.parse_output(runner.cat_output()):
                # Final format: "Location" "Avg"
                fout.write(f"\"{loc}\" \"{avg_str}\"\n")

if __name__ == "__main__":
    run_job_to_file(MRTop3ByAvgAnnualRainYes, INPUT_NAME, OUTPUT_NAME)
