from concurrent import futures
import logging
import sys
import grpc

# import helloworld_pb2
# import helloworld_pb2_grpc
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_stubs"))
sys.path.append(os.path.dirname(__file__))
import server_pb2 as server_pb2
import server_pb2_grpc as server_pb2_grpc


class RMServer(server_pb2_grpc.RMServerServicer):
    def __init__(self, input_val):
        """
        Input value
        """
        self.worker_dict = dict()
        self.worker_dict[1] = 2

    def QueryWorker(self, request, context):
        return server_pb2.IntVal(value=2)


def serve():
    port = "50051"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    # helloworld_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
    server_pb2_grpc.add_RMServerServicer_to_server(RMServer(4), server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
