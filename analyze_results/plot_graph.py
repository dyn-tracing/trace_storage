import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

def import_csv(filename):
    with open(filename) as csvfile:
        spamreader = csv.reader(csvfile)
        to_return = []

        for row in spamreader:
            to_return.extend(row)
    return to_return

def import_query_data(query):
    with open(filename) as csvfile:
        spamreader = csv.reader(csvfile)
        latencies = []

        for row in spamreader:
            if query in row[1]:
                latencies.append((row[0], row[2]))

        latencies.sort()
        to_return_lat = []
        for l in latencies:
            to_return_lat.append(l[1])
        import pdb; pdb.set_trace()
        return to_return_lat

# We make two graphs - latency by num traces, latency by bytes.
bytes_count = import_csv("bytes_count.csv")
traces_count = import_csv("traces_count.csv")


queries = ["duration", "fanout", "one_call", "height"]
latencies = []
for query in queries:
    latencies.append(import_query_data(query))

fig, ax = plt.subplots()

plt.plot(bytes_count, new_x_duration, label = "duration", linestyle='--', marker='o', color='b')
plt.plot(bytes_count, new_x_fanout, label = "fanout", linestyle='--', marker='o', color='r')
plt.plot(bytes_count, new_x_one_other_call, label = "one other call", linestyle='--', marker='o', color='g')
plt.plot(bytes_count, new_x_height, label = "height", linestyle='--', marker='o', color='c')
plt.title("AliBaba Query Latencies")
plt.ylabel("Latency (s)")
plt.xlabel("Number of AliBaba Traces (in thousands)")
plt.ylim(0, 60)
plt.legend()
plt.show()
plt.savefig('graph.png')
