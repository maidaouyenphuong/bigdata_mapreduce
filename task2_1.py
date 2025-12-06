from mrjob.job import MRJob
from pathlib import Path

INPUT_NAME = "observations.txt"
OUTPUT_NAME = "output2_1.txt"

class MRTop3ByLocation(MRJob):

    #MAP: line => (location, (date, max_temp_float, max_temp_str))
    def mapper(self, _, line: str):
        #split values by commas, remove whitespace
        parts = [p.strip() for p in line.strip().split(",")]
        
        #extract values
        location = parts[0]
        date = parts[1]
        max_temp_str = parts[3]
        
        #convert max_temp as float for comparison, but still keep original max_temp_str for printing
        max_temp = float(max_temp_str)
        
        #key = location, value = tuple (date, max_temp_float, max_temp_str)
        yield location, (date, max_temp, max_temp_str)

    #REDUCE: keep top-3 hottest days for each location
    def reducer(self, location, values):
        top3 = []
        for date, t_float, t_str in values:
            item = (t_float, date, t_str)
            #add new record to list
            top3.append(item)
            #sort list in descending order,
            #if two temperature are equal, back to compare the date (next element)
            top3.sort(reverse=True)
            #if we have more than 3 items, remove the last one (the smallest)
            if len(top3) > 3:
                top3.pop()   
        for t_float, date, t_str in top3:
            # normal (key, value) output; we'll format when writing to file
            yield location, f"{date},{t_str}"

# Write to file 
# No need to run in cmd
# I run in VSCode and it will automatically create the file and write output into the file
def run_job_to_file(job_cls, in_name, out_name):
    in_path = str(Path.cwd() / in_name)
    out_path = Path.cwd() / out_name
    job = job_cls(args=["--no-conf", in_path])
    with job.make_runner() as runner:
        runner.run()
        with out_path.open("w", encoding="utf-8", newline="") as f:
            for location, value in job.parse_output(runner.cat_output()):
                # Final format: "Location" "Date,MaxTemp"
                f.write(f"\"{location}\" \"{value}\"\n")

if __name__ == "__main__":
    run_job_to_file(MRTop3ByLocation, INPUT_NAME, OUTPUT_NAME)
