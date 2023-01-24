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
        # the reason for the weirdness at the end is that I did every 5,
        # but 144 isn't divisible by 5, so I did it once we had all data in the
        # system. Then 22 and 30 were messed up so I added that and did it again
        # So now we have two tacked on and the end that aren't divisible by 5
        for i in range(len(nums)):
            if (i+1) % 5 == 0 or i == 141 or i == 143: # want to include the last one
                to_return.append(int(nums[i]))

    return to_return

def import_query_data(query):
    with open("processed.csv") as csvfile:
        spamreader = csv.reader(csvfile)
        latencies = []

        for row in spamreader:
            if query in row[1]:
                if row[0] == "last":
                    latencies.append((145, row[2]))
                else:
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
#fig.tight_layout()
#plt.ylim(0, 60)

for ax in axs:
    box = ax.get_position()
    ax.set_position([box.x0, box.y0 + box.height * 0.15,
                 box.width, box.height * 0.85])

axs[0].set_title("Structural Query Latencies on AliBaba Data")
axs[1].legend(loc='upper center', ncol=4, bbox_to_anchor=(0.5, -0.3), fancybox=True, shadow=True)

plt.savefig('graph.pdf')
plt.show()
