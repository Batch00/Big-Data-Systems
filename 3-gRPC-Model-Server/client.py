import sys
import grpc
import modelserver_pb2_grpc, modelserver_pb2
import threading
import pandas as pd

# hit and miss counts
hit_count = 0
miss_count = 0
lock = threading.Lock()


def calc_hits(csv):
    global hit_count, miss_count, lock
    df = pd.read_csv(csv, header=None)
    for index, row in df.iterrows():
        # Make Predict call to the server
        response = stub.Predict(modelserver_pb2.PredictRequest(X=row.values.tolist()))
        with lock:
            if response.hit:
                hit_count += 1
            else:
                miss_count += 1


channel = grpc.insecure_channel("localhost:5440")
stub = modelserver_pb2_grpc.ModelServerStub(channel)

# Set coefficients
coefs = [float(val) for val in sys.argv[2].split(",")]
stub.SetCoefs(modelserver_pb2.SetCoefsRequest(coefs=coefs))

# Create threads
threads = []
for csv in sys.argv[3:]:
    thread = threading.Thread(target=calc_hits, args=(csv,))
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

# Calculate hit rate
total = hit_count + miss_count
hit_rate = hit_count / total
print(hit_rate)
