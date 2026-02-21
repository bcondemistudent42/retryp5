from abc import ABC, abstractmethod
from time import process_time
from random import randint
from typing import Any, Union, Protocol


class ProcessingPipeline(ABC):
    def __init__(self, type_file: str, pipeline_id: int) -> None:
        self.file_type = type_file
        self.pip_id = pipeline_id
        self.pip = [InputStage, TransformStage, OutputStage]

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        pass


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        pass


class InputStage:
    def process(data: Any) -> dict[Any, Any]:
        if not isinstance(data, str) and not isinstance(data, dict):
            raise ValueError("The data must be STR on DICT type")
        print(f"Input: '{data}'")
        return {}


class TransformStage:
    def process(data: Any) -> dict[Any, Any]:
        if isinstance(data, dict):
            print("Transform: Enriched with metadata and validation")
            data["type"] = 0
            return data
        elif isinstance(data, str):
            if "," in data:
                print("Transform: Parsed and structured data")
                output = {x: randint(0, 100) for x in data.split(",")}
                output["type"] = 1
                return output
            print("Transform: Aggregated and filtered")
            output = {
                      "reads": randint(0, 100),
                      "avg": round(randint(0, 45)),
                      "type": 2
            }
            return output
        else:
            print("The data type is not managed")
            return {}


class OutputStage:
    def process(data: Any) -> str:
        if data["type"] == 0:
            val = data["value"]
            if val > 15 and val < 35:
                txt = "Output: Processed temperature reading:"
                txt1 = f" {val} °C (Normal range)"
                return txt + txt1
            else:
                txt = "Output: Processed temperature reading:"
                txt1 = f" {data['value']} °C (Out of range)"
                return txt + txt1
        if data["type"] == 1:
            nb_action = data["action"]
            txt = "Output: User activity logged:"
            txt1 = f" {nb_action} actions processed"
            return txt + txt1
        elif data["type"] == 2:
            txt = f"Output: Stream summary: {randint(1, 10)} readings "
            t1 = f"{data['reads']}°C"
            t2 = ", avg: "
            return txt + t2 + t1
        raise ValueError("Error: data have wrong type")


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pip_id: int) -> None:
        super().__init__("JSON", pip_id)

    def process(self, data: Any) -> Union[str, Any]:
        print(f"Processing {self.file_type} data through pipeline...")
        try:
            for elt in self.pip:
                if (elt == TransformStage):
                    data = elt.process(data)
                else:
                    elt.process(data)
            print(f"Process time == {round(process_time(), 5)}")
        except Exception as e:
            print(f"Haddling error: {e}")
        return None


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pip_id: int) -> None:
        super().__init__("CSV", pip_id)

    def process(self, data: Any) -> Union[str, Any]:
        print(f"Processing {self.file_type} data through pipeline...")
        try:
            for elt in self.pip:
                if (elt == TransformStage):
                    data = elt.process(data)
                else:
                    elt.process(data)
            print(f"Process time == {round(process_time(), 5)}")
        except Exception as e:
            print(f"Handling error: {e}")
        return None


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pip_id: int) -> None:
        super().__init__("STREAM", pip_id)

    def process(self, data: Any) -> Union[str, Any]:
        print(f"Processing {self.file_type} data through pipeline...")
        try:
            for elt in self.pip:
                if (elt == TransformStage):
                    data = elt.process(data)
                else:
                    elt.process(data)
            print(f"Process time == {round(process_time(), 5)}")
        except Exception as e:
            print(f"Hadnling error: {e}")
        return None


class NexusManager:
    def __init__(self) -> None:
        self.dct: dict[str, Any] = {}

    def add_pipeline(self, id: str, elt: Any) -> None:
        try:
            if not isinstance(id, str):
                raise ValueError
        except ValueError:
            print("The id must be an STR and elt an Instance")
            return
        try:
            self.dct[id] = elt
        except Exception as e:
            print(f"Error handled: {e}")

    def process_data(self, data: Any, id: str) -> Any:
        try:
            output = self.dct[id].process(data)
        except Exception:
            print("Error in this pipline STOPING process")
            return output


def main() -> None:
    print(
        "=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n\n"
        "Initializing Nexus Manager...\n"
        "Pipeline capacity: 1000 streams/second\n"
        "Creating Data Processing Pipeline...\n"
        "Stage 1: Input validation and parsing\n"
        "Stage 2: Data transformation and enrichment\n"
        "Stage 3: Output formatting and delivery\n\n"
        "=== Multi-Format Data Processing ===\n"
    )
    input_json = {"sensor": "temp", "value": 23.5, "unit": "C"}
    json = JSONAdapter(1)
    json.process(input_json)
    print()

    input_csv = "user,action,timestamp"
    csv = CSVAdapter(1)
    csv.process(input_csv)
    print()

    input_stream = "Real-time sensor stream"
    stream = StreamAdapter(1)
    stream.process(input_stream)
    print()

    print("======== Testing with Nexus Manager ============\n")
    nexus = NexusManager()
    nexus.add_pipeline("JSON01", json)
    data = nexus.process_data(input_json, "JSON01")

    print()
    new_data = nexus.process_data(data, "JSON01")

    print()
    nexus.process_data(new_data, "JSON01")

    print()
    nexus.add_pipeline("CSV01", csv)
    nexus.process_data(input_csv, "CSV01")

    print()
    nexus.add_pipeline("STREAM01", stream)
    nexus.process_data(input_stream, "STREAM01")
    print()
    return


if __name__ == "__main__":
    main()
