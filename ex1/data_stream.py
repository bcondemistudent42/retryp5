from typing import Any, List, Optional, Dict, Union
from abc import ABC, abstractmethod


class DataStream(ABC):
    def __init__(self, id: str) -> None:
        if not isinstance(id, str):
            raise ValueError("STREAM_ID must be an STR")
        self.id = id

    @abstractmethod
    def process_batch(self,  data_batch: List[Any]) -> str:
        return ""

    def filter_data(self, data_batch: List[Any], criteria: str) -> List[Any]:
        print("Default implementation")
        return []

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        print("Default implementation")
        return {}


class EventStream(DataStream):
    def __init__(self, id: str) -> None:
        print("Initializing Event Stream...")
        self.data_type = "System Events"
        super().__init__(id)

    def process_batch(self, data_batch: List[Any]) -> str:
        if not isinstance(data_batch, list):
            raise ValueError("Databatch have to be a list")
        for elt in data_batch:
            if elt != "login" and elt != "error" and elt != "logout":
                raise ValueError("One element in list is not correct")
        self.data = data_batch
        txt = f"Stream ID: {self.id}, Type: {self.data_type}\n"
        txt1 = f"Processing event batch: {data_batch}"
        return txt + txt1

    def filter_data(self, data_batch: List[Any], criteria: Optional[str]
                    = None) -> List[Any]:
        if not isinstance(data_batch, list) or not isinstance(criteria, str):
            t = "One of the parameters doesnt have the right type"
            raise ValueError(t)
        output = [i for i, x in enumerate(self.data) if x == criteria]
        return output

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        cnt = sum([1 for x in self.data if x == "error"])
        output = {str(x): i for i, x in enumerate(self.data) if x == "error"}
        print(f"Event analysis: {len(self.data)} events, {cnt} error detected")
        return output


class TransactionStream(DataStream):
    def __init__(self, id: str) -> None:
        print("Initializing Transaction Stream...")
        self.data_type = "Financial Data"
        super().__init__(id)

    def process_batch(self, data_batch: List[Any]) -> str:
        if not isinstance(data_batch, list):
            raise ValueError("'data_batch' is not a list")
        try:
            self.dct = {
                int(x.split(":")[1]): x.split(":")[0]
                for x in data_batch if x.startswith(("buy", "sell"))
            }
        except Exception:
            raise ValueError("One element of the list is not correct")
        if len(data_batch) != len(self.dct.keys()):
            raise ValueError("One element of the list is not correct")
        txt = f"Stream ID: {self.id}, Type: {self.data_type}\n"
        txt1 = f"Processing transaction batch: {data_batch}"
        return txt + txt1

    def filter_data(self, data_batch: List[Any], criteria: str) -> List[Any]:
        if not isinstance(criteria, str):
            raise ValueError("'criteria' is not a str")
        lst_values = [
            x for x in self.dct.keys() if self.dct[x] == criteria
        ]
        return lst_values

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        result = sum([x for x in self.dct.keys() if self.dct[x] == "buy"])
        result -= sum([x for x in self.dct.keys() if self.dct[x] == "sell"])
        sign = "+"
        if result < 0:
            sign = "-"
        txt = f"Transaction analysis: {len(self.dct.keys())} operations,"
        txt1 = f"net flow: {sign}{result} units"
        print(txt + txt1)
        return {
                result: f"The result is {result}",
                -1: len(self.dct.keys())
               }


class SensorStream(DataStream):
    def __init__(self, id: str) -> None:
        print("Initializing Sensor Stream..")
        self.data_type = "Environmental Data"
        super().__init__(id)

    def process_batch(self, data_batch: List[Any]) -> str:
        if not isinstance(data_batch, list):
            raise ValueError("'data_batch' is not a list")
        txt = f"Stream ID: {self.id}, Type: {self.data_type}"
        txt1 = f"\nProcessing sensor batch: {data_batch}"
        try:
            self.dct = {
                x.split(":")[0]: float(x.split(":",)[1])
                for x in data_batch
            }
        except Exception:
            raise ValueError("One element of the list is not correct")
        return txt + txt1

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if not isinstance(data_batch, list):
            raise ValueError("'data_batch' is not a list")
        output = [
            x.split(":")[1] for x in data_batch
            if x.split(":")[0] == criteria
        ]
        return output

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        read = len(self.dct.keys())
        avr = self.dct["temp"]
        print(f"Sensor analysis: {read} readings processed, avg temp: {avr}°C")
        my_dict = {self.id: float(read)}
        return my_dict


class StreamProcessor():
    def process_batch(self, stream: DataStream, data: List[Any]) -> str:
        if not isinstance(stream, DataStream):
            print(f"[ERROR] Invalid stream type: {type(stream)}")
            return
        return stream.process_batch(data)


def data_stream() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")
    lst_sensor = ["temp:22.5", "humidity:65", "pressure:1013"]
    sensor = SensorStream("Sensor001")
    manager = StreamProcessor()
    print(manager.process_batch(sensor, lst_sensor))
    sensor.get_stats()
    print(sensor.filter_data(lst_sensor, "pressure"))

    print()
    lst_transac = ["buy:100", "sell:150", "buy:75"]
    transaction = TransactionStream("Transac001")
    print(transaction.process_batch(lst_transac))
    transaction.get_stats()
    print(transaction.filter_data(lst_transac, "buy"))
    print()

    event = EventStream("Event001")
    print(event.process_batch(["login", "error", "logout"]))
    event.get_stats()
    print(event.filter_data(["login", "error", "logout"], "error"))

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface..")
    print()
    print("Batch 1 Results:")
    r_sensor = int(len(sensor.get_stats().keys()))
    r_transact = int(transaction.get_stats()[-1])
    r_event = int(len(event.get_stats()))
    print(f"-Sensor data: {r_sensor} readings processed")
    print(f"-Transaction data: {r_transact} operations processed")
    print(f"-Event data: {r_event} events processed")
    print("\n All streams processed successfully. Nexus throughput optimal.")
    return


if __name__ == "__main__":
    data_stream()
