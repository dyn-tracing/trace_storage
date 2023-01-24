
csv_hdr = ["number_of_csvs", "query", "median_time(ms)"]

queries = ["duration", "fanout", "height", "one_call"]

all_files = []
for i in range(1, 145):
    if i%5 == 0:
        all_files.append(i)
all_files.append(144)
all_files.append("last")
#all_files = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]

results = ",".join(csv_hdr) + "\n"

for file_num in all_files:
    for q in queries:
        txt_file = f"{file_num}{q}.txt"

        with open(txt_file) as f:
            data = f.readlines()[-1]
            print(data)
            if not data.startswith("Median: "):
                print("Something wrong!")
                print(txt_file)
                exit(0)
            
            median = float((data.split(" ")[1]).strip('\n'))

            line_to_insert = f"{file_num},{file_num}{q},{median}" + "\n"
            results += line_to_insert


with open("processed.csv", "w") as f:
    f.write(results)

