
url='http://alitrip.oss-cn-zhangjiakou.aliyuncs.com/TraceData'

for VAR in 6 7 8 9 10 11 12 13 14 15 16
do
	cd ../microservices_env/send_alibaba_data_to_gcs
	curl ${url}/MSCallGraph/MSCallGraph_${VAR}.tar.gz -o MSCallGraph_${VAR}.tar.gz
	tar -xvzf MSCallGraph_${VAR}.tar.gz

	./alibaba_to_gcs ./MSCallGraph_${VAR}.csv 2>&1 | tee ${VAR}alibaba.txt

	echo "\n"
	
	cd ../../trace_storage

	bazel run --cxxopt=-std=c++17 :graph_query 10 duration 2>&1 | tee ./results/${VAR}duration.txt

	echo "\n"

	bazel run --cxxopt=-std=c++17 :graph_query 10 fanout 2>&1 | tee ./results/${VAR}fanout.txt

	echo "\n"

	bazel run --cxxopt=-std=c++17 :graph_query 10 height 2>&1 | tee ./results/${VAR}height.txt

	echo "\n"

	bazel run --cxxopt=-std=c++17 :graph_query 10 one_call 2>&1 | tee ./results/${VAR}one_call.txt
	

	cd ../microservices_env/send_alibaba_data_to_gcs
	rm MSCallGraph_${VAR}.csv
	rm MSCallGraph_${VAR}.tar.gz
done
