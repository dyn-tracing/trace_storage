
FOLDER='results'
cd ../${FOLDER}
python3 ../analyze_results/process_results.py
cp processed.csv ../analyze_results/
cd ../../microservices_env/send_alibaba_data_to_gcs
python3 count_traces.py
python3 count_bytes.py
cp traces_count.csv ../../trace_storage/analyze_results/
cp bytes_count.csv ../../trace_storage/analyze_results/

python3 plot_graph.py
