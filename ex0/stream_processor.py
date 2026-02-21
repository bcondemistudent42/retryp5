from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def print_processing(self, data: Any) -> None:
        print(f"Processing data: '{data}'")

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        print("Initializing Numeric Processor...")

    def process(self, data: Any) -> str:
        for i in data:
            if not isinstance(i, int):
                raise ValueError("One or many elements are not INT")
        avg = round(sum(data)/len(data), 1)
        suum = sum(data)
        return f"Processed {len(data)} numeric values, sum={suum}, avg={avg}"

    def validate(self, data: Any) -> bool:
        for i in data:
            if not isinstance(i, int):
                raise ValueError("One or many elements are not INT")
        print("Validation: Numeric data verified")
        return True

    def format_output(self, result: str) -> str:
        return "Numeric Processor " + super().format_output(result)


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        print("Initializing Text Processor...")

    def process(self, data: Any) -> str:
        if not isinstance(data, str):
            raise ValueError("Data must be str")
        lenn = len(data)
        words = len(data.split())
        return f"Processed text: {lenn} characters, {words} words"

    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            raise ValueError("This data is not an STR")
        print("Validation: Text data verified")
        return isinstance(data, str)

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        print("Initializing Log Processor...")

    def process(self, data: Any) -> str:
        if not isinstance(data, str):
            raise ValueError("Logs must be of type str")
        for log_type in ["ERROR", "INFO"]:
            if data.startswith(log_type):
                txt = f"[{data.split(':')[0]}] {data.split(':')[0]}"
                txt1 = f" level detected: {str(data.split(':')[1])}"
                return txt + txt1
        raise ValueError("Error : The given str is not in LOG format")

    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            raise ValueError("Logs must be of type str")
        for log_type in ["ERROR", "INFO"]:
            if data.startswith(log_type):
                print("Validation: Log entry verified")
                return True
        return False

    def format_output(self, result: str) -> str:
        return super().format_output(result)


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()
    lst = [1, 2, 3, 4, 5]
    log = "ERROR: Connection timeout"
    my_str = "Hello Nexus World"
    test_lst = NumericProcessor()
    test_lst.print_processing(lst)
    output = test_lst.process(lst)
    test_lst.validate(lst)
    print(test_lst.format_output(output))

    print()
    test_txt = TextProcessor()
    test_txt.print_processing(my_str)
    output = test_txt.process(my_str)
    test_txt.validate(my_str)
    print(test_txt.format_output(output))
    print()

    test_log = LogProcessor()
    test_log.print_processing(log)
    output = test_log.process(log)
    test_log.validate(log)
    print(test_log.format_output(output))

    print()
    print("=== Polymorphic Processing Demo ===")
    print()

    print("Processing multiple data types through same interface...")
    big_lst = [([1, 2, 3], test_lst),
               ("Heelllooo 42", test_txt),
               ("INFO: System ready", test_log)]
    i = 1
    for elt in big_lst:
        my_class = elt[1]
        output = my_class.process(elt[0])
        print(f"Result {i}: {output}")
        i += 1
    print()
    print("Foundation systems online. Nexus ready for advanced streams")
    return


if __name__ == "__main__":
    main()
