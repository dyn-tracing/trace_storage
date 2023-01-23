import matplotlib.pyplot as plt
import numpy as np
import csv

def import_csv(filename):
    with open(filename) as csvfile:
        spamreader = csv.reader(csvfile)
        nums = []

        for row in spamreader:
            nums.extend(row)

        to_return = []
        for i in range(len(nums)):
            if (i+1) % 5 == 0 or i == 29:
                to_return.append(int(nums[i]))

    return to_return

def import_query_data(query):
    with open("processed.csv") as csvfile:
        spamreader = csv.reader(csvfile)
        latencies = []

        for row in spamreader:
            if query in row[1]:
                latencies.append((int(row[0]), row[2]))

        latencies.sort()
        to_return_lat = []
        for l in latencies:
            to_return_lat.append(float(l[1])/1000.0) # get into seconds
        return to_return_lat

# We make two graphs - latency by num traces, latency by bytes.
bytes_count = import_csv("bytes_count.csv")
bytes_count = [x*0.000001 for x in bytes_count] # convert to MB

traces_count = import_csv("traces_count.csv")
traces_count = [x/1000.0 for x in traces_count] # convert to thousands


queries = ["duration", "fanout", "one_call", "height"]
latencies = []
for query in queries:
    latencies.append(import_query_data(query))
fig, axs = plt.subplots(2)

for query in range(len(queries)):
    axs[0].plot(bytes_count, latencies[query], label = queries[query])
    axs[1].plot(traces_count, latencies[query], label = queries[query])

#plt.plot(bytes_count, new_x_duration, label = "duration", linestyle='--', marker='o', color='b')
#plt.plot(bytes_count, new_x_fanout, label = "fanout", linestyle='--', marker='o', color='r')
#plt.plot(bytes_count, new_x_one_other_call, label = "one other call", linestyle='--', marker='o', color='g')
#plt.plot(bytes_count, new_x_height, label = "height", linestyle='--', marker='o', color='c')
axs[0].set(xlabel='Bytes of AliBaba Data (MB)', ylabel='Latency (s)')
axs[1].set(xlabel='Number of Traces (Thousands)', ylabel='Latency (s)')
fig.tight_layout()
#plt.ylim(0, 60)
plt.legend()
plt.savefig('graph.pdf')
plt.show()
