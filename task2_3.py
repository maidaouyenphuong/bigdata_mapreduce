from mrjob.job import MRJob
from mrjob.step import MRStep
from pathlib import Path
from datetime import datetime, date, timedelta

INPUT_NAME = "observations.txt"
OUTPUT_NAME = "output2_3.txt"

#Convert 'YYYY-MM-DD' to a date object for accurate sorting
def to_date(s: str) -> date:
    return datetime.strptime(str(s).strip()[:10], "%Y-%m-%d").date()
#Convert to float 
def to_float(x):
    try:
        return float(x)
    except Exception:
        return None
    
#Compute severity points a single day adds ("Daily Contribution")
def day_contrib(max_temp, rainfall) -> float:
    #if either max_temp or rainfall is None, replace with 0.0
    mt = 0.0 if max_temp is None else max_temp
    rf = 0.0 if rainfall is None else rainfall
    #only count max_temp > 35. if max_temp > 36, contribution is (max_temp - 35); otherwise, contribution is 0
    #only count rainfall > 20. if rainfall > 20, contribution is (rainfall - 20); otherwise, contribution is 0 
    return max(0.0, mt - 35.0) + max(0.0, rf - 20.0)

#Check if a day qualifies as extreme
def is_extreme(max_temp, rainfall) -> bool:
    #if max_temp is not None and > 35, mt = True; otherwise, mt = False
    mt = (max_temp is not None) and (max_temp > 35.0)
    #if rainfall is not None and > 20, rf = True, otherise, rf = False
    rf = (rainfall is not None) and (rainfall > 20.0)
    return mt or rf

#Check for the longest chain
def best_chain_for_group(day_map: dict):
    #sort all available dates
    dates = sorted(day_map.keys())
    #store the best chain found so far
    best = None
    #start date of the current chain
    cur_start = None
    #end date of the current chain
    cur_end = None
    #total severity of the current chain
    cur_sev = 0.0
    #length of the current chain (number of days)
    cur_len = 0
    #check whether today is consecutive to the previous day
    prev_date = None

    #check whether the chain just finished is better than the "best" chain
    def maybe_commit(current):
        nonlocal best
        #if best is empty, set it to current
        if best is None:
            best = current
            return
        #if best is not empty, compare longest chain (length desc)
        #if equal length, prefer higher severity (severity desc)
        #if still tied, prefer chain that starts earlier (start asc)
        if (current["length"] > best["length"] or
            (current["length"] == best["length"] and current["severity"] > best["severity"]) or
            (current["length"] == best["length"] and current["severity"] == best["severity"] and current["start"] < best["start"])):
            best = current

    #loop through all dates in chronological order
    for d in dates:
        max_t, rain = day_map[d]
        #if day is extreme
        if is_extreme(max_t, rain):
            #if not currently in a chain, start a new chain
            if cur_len == 0:
                cur_start = d
                cur_end = d
                cur_sev = day_contrib(max_t, rain)
                cur_len = 1
            #if already in a chain, check whether today is consecutive to yesterdaya
            else:
                if prev_date is not None and d == prev_date + timedelta(days=1):
                    #extend the current chain
                    cur_end = d
                    cur_sev += day_contrib(max_t, rain)
                    cur_len += 1
                else:
                    #yesterday is not directly before today, start a new one, commit old chain
                    maybe_commit({
                        "start": cur_start,
                        "end": cur_end,
                        "severity": round(cur_sev, 2),
                        "length": cur_len
                    })
                    cur_start = d
                    cur_end = d
                    cur_sev = day_contrib(max_t, rain)
                    cur_len = 1
        #if day is not extreme
        else:
            #if we were building a chain, commit it
            if cur_len > 0:
                maybe_commit({
                    "start": cur_start,
                    "end": cur_end,
                    "severity": round(cur_sev, 2),
                    "length": cur_len
                })
                cur_start = cur_end = None
                cur_sev = 0.0
                cur_len = 0
        #update to remember what yesterday was
        prev_date = d

    #if we ended while still inside a chain, commit one last time
    if cur_len > 0:
        maybe_commit({
            "start": cur_start,
            "end": cur_end,
            "severity": round(cur_sev, 2),
            "length": cur_len
        })

    return best

class MRExtremeChainsTop5(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_extract,
                reducer=self.reducer_best_per_group
            ),
            MRStep(
                reducer=self.reducer_global_top5
            ),
        ]

    # MAP: read CSV, emit ((Location, Year), (date_iso, max_temp, rainfall))
    def mapper_extract(self, _, line: str):
        parts = line.strip().split(",")
        loc = parts[0].strip()
        # Guard bad rows
        try:
            d = to_date(parts[1])
        except Exception:
            return
        max_temp = to_float(parts[3])  # MaxTemp
        rainfall = to_float(parts[4])  # Rainfall
        key = (loc, d.year)
        # emit json-serializable values
        yield key, (d.isoformat(), max_temp, rainfall)

    # REDUCE (per (Location, Year)): compute best chain for this group, pass a concise candidate to the final reducer
    def reducer_best_per_group(self, key, values):
        # Rebuild day_map
        # key = (loc, year)
        day_map = {}
        for date_iso, max_temp, rainfall in values:
            try:
                d = to_date(date_iso)
            except Exception:
                continue
            day_map[d] = (max_temp, rainfall)

        best = best_chain_for_group(day_map)
        if best is None:
            return

        loc, yr = key
        # Emit to a single global key so step 2 sees all candidates
        # Pack a record that's easy to sort later
        record = {
            "loc": loc,
            "year": yr,
            "start": best["start"].isoformat(),
            "end": best["end"].isoformat(),
            "severity": float(best["severity"]),
            "length": int(best["length"]),
        }
        yield "ALL", record

    # FINAL REDUCER: pick global Top-5 and output formatted lines
    def reducer_global_top5(self, _all_key, records):
        # Collect, sort by (-severity, -length, start asc), take top 5
        buf = []
        for r in records:
            buf.append(r)

        buf.sort(key=lambda r: (-r["severity"], -r["length"], r["start"]))

        top5 = buf[:5]
        for r in top5:
            # Key = location, Value = dict for easy local formatting.
            yield r['loc'], {
                "year": r["year"],
                "start": r["start"],
                "end": r["end"],
                "severity": float(f"{r['severity']:.2f}"),
                "length": r["length"],
            }

# Write to file 
# No need to run in cmd
# I run in VSCode and it will automatically create the file and write output into the file
def run_mapreduce_locally(in_name=INPUT_NAME, out_name=OUTPUT_NAME):
    job = MRExtremeChainsTop5(args=[in_name])
    with job.make_runner() as runner:
        runner.run()
        out_path = Path(out_name)
        # newline="" avoids the extra blank lines on Windows
        with out_path.open("w", encoding="utf-8", newline="") as f:
            # parse_output() gives you Python (key, value) decoded from the job's protocol
            for loc, rec in job.parse_output(runner.cat_output()):
                # Format exactly like before, but done locally here:
                line = f"\"{loc}\" \"{rec['year']} | {rec['start']} | {rec['end']} | {float(rec['severity']):.2f}\""
                # We already format the whole value; just write it through.
                f.write(line + "\n")

if __name__ == "__main__":
    run_mapreduce_locally()
