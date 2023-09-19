from pyarrow import flight

if __name__ == "__main__":
    flight_client = flight.FlightClient("grpc://127.0.0.1:8888")
