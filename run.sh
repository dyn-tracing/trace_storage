bazel run --cxxopt=-std=c++17 :graph_query 10 duration > ./results/5duration.txt

echo "\n"

bazel run --cxxopt=-std=c++17 :graph_query 10 fanout > ./results/5fanout.txt

echo "\n"

bazel run --cxxopt=-std=c++17 :graph_query 10 one_call > ./results/5one_call.txt

echo "\n"

bazel run --cxxopt=-std=c++17 :graph_query 10 height > ./results/5height.txt

