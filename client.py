from concurrent import futures
import logging
import time
import grpc

# import helloworld_pb2
# import helloworld_pb2_grpc
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_stubs"))
sys.path.append(os.path.dirname(__file__))
import server_pb2 as server_pb2
import server_pb2_grpc as server_pb2_grpc


def run():
    for _ in range(30):
        time.sleep(2)
        start_time = time.time()
        with grpc.insecure_channel(f"{sys.argv[1]}:50051") as channel:
            stub = server_pb2_grpc.RMServerStub(channel)
            response = stub.QueryWorker(server_pb2.IntVal(value=1))
            print(response.value)
        end_time = time.time()
        print(end_time - start_time)
        logger.info(end_time - start_time)


if __name__ == "__main__":
    import logging

    logging.basicConfig(filename="timing_file.py")
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # logger.info(args)
    run()
