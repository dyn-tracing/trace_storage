
url='http://alitrip.oss-cn-zhangjiakou.aliyuncs.com/TraceData'
FOLDER='batched_index_all'
mkdir ./${FOLDER}
for VAR in {16..144}
do
	if [[ ${VAR} -eq 16 ]]
	then
		cd ../microservices_env/send_alibaba_data_to_gcs
	fi
	curl ${url}/MSCallGraph/MSCallGraph_${VAR}.tar.gz -o MSCallGraph_${VAR}.tar.gz
	tar -xvzf MSCallGraph_${VAR}.tar.gz

	./alibaba_to_gcs ./MSCallGraph_${VAR}.csv 2>&1 | tee ${VAR}alibaba.txt

	if [[ ${VAR}%5 -eq 0 ]]
	then
		echo "\n"
		
		cd ../../trace_storage

		bazel run --cxxopt=-std=c++17 :graph_query 10 duration 2>&1 | tee ./${FOLDER}/${VAR}duration.txt

		echo "\n"

		bazel run --cxxopt=-std=c++17 :graph_query 10 fanout 2>&1 | tee ./${FOLDER}/${VAR}fanout.txt

		echo "\n"

		bazel run --cxxopt=-std=c++17 :graph_query 10 height 2>&1 | tee ./${FOLDER}/${VAR}height.txt

		echo "\n"

		bazel run --cxxopt=-std=c++17 :graph_query 10 one_call 2>&1 | tee ./${FOLDER}/${VAR}one_call.txt
		

		cd ../microservices_env/send_alibaba_data_to_gcs
	fi
	rm MSCallGraph_${VAR}.csv
	rm MSCallGraph_${VAR}.tar.gz
done
