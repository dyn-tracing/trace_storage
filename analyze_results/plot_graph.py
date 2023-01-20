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
            if (i+1) % 5 == 0:
                print("i is ", i)
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
            to_return_lat.append(float(l[1]))
        return to_return_lat

# We make two graphs - latency by num traces, latency by bytes.
bytes_count = import_csv("bytes_count.csv")
traces_count = import_csv("traces_count.csv")


queries = ["duration", "fanout", "one_call", "height"]
latencies = []
for query in queries:
    latencies.append(import_query_data(query))

fig, ax = plt.subplots()

for query in range(len(queries)):
    plt.plot(bytes_count, latencies[query], label = queries[query])

#plt.plot(bytes_count, new_x_duration, label = "duration", linestyle='--', marker='o', color='b')
#plt.plot(bytes_count, new_x_fanout, label = "fanout", linestyle='--', marker='o', color='r')
#plt.plot(bytes_count, new_x_one_other_call, label = "one other call", linestyle='--', marker='o', color='g')
#plt.plot(bytes_count, new_x_height, label = "height", linestyle='--', marker='o', color='c')
plt.title("AliBaba Query Latencies")
plt.ylabel("Latency (ms)")
plt.xlabel("Bytes of AliBaba Data")
#plt.ylim(0, 60)
plt.legend()
plt.show()
plt.savefig('graph.png')
