from collections import OrderedDict
import torch
import threading
import grpc
from concurrent import futures
import modelserver_pb2_grpc, modelserver_pb2


class PredictionCache:
    def __init__(self):
        self.coefs = None
        self.cache = OrderedDict()
        self.cache_limit = 10
        self.lock = threading.Lock()

    def SetCoefs(self, coefs):
        with self.lock:
            self.coefs = coefs
            # Clear Cache
            self.cache.clear()

    def Predict(self, X):
        with self.lock:
            # Round X
            X = torch.round(X, decimals=4)
            # Convert to Tuple
            key = tuple(X.flatten().tolist())
            # Check if in Cache
            if key in self.cache:
                return self.cache[key], True
            else:
                y = X @ self.coefs
            # Make sure Cache does not exceed 10 items
            if len(self.cache) >= self.cache_limit:
                self.cache.popitem(last=False)

            self.cache[key] = y

            return y, False


class ModelServer(modelserver_pb2_grpc.ModelServerServicer):
    def __init__(self):
        self.cache = PredictionCache()

    def SetCoefs(self, request, context):
        # translate between the repeated float values and tensor
        coefs = torch.tensor(request.coefs, dtype=torch.float32).reshape(-1, 1)
        self.cache.SetCoefs(coefs)
        return modelserver_pb2.SetCoefsResponse(error="")

    def Predict(self, request, context):
        # translate between the repeated float values and tensor
        X = torch.tensor(request.X, dtype=torch.float32).reshape(1, -1)
        y, hit = self.cache.Predict(X)
        response = modelserver_pb2.PredictResponse(y=y, hit=hit, error="")
        return response


server = grpc.server(
    futures.ThreadPoolExecutor(max_workers=4), options=(("grpc.so_reuseport", 0),)
)
modelserver_pb2_grpc.add_ModelServerServicer_to_server(ModelServer(), server)
server.add_insecure_port(
    "0.0.0.0:5440",
)
server.start()
server.wait_for_termination()
