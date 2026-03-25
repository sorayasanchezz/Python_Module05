from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate if data is appropriate for this processor"""
        pass

    @abstractmethod
    def process(self, data: Any) -> str:
        """Process the data and return result string"""
        pass

    def format_output(self, result: str) -> str:
        """Format the output string"""
        return f"Output: {result}"


# Specialized classes (no constructor parameters required)
class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        val: bool = (
            isinstance(data, list)
            and all(isinstance(n, (int, float)) for n in data)
            and len(data) > 0
        )
        return val

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid numeric data")

            nbr: int = len(data)
            total: float = sum(data)
            avg: float = total / nbr

            return f"Processed {nbr} numeric values, sum={total}, avg={avg}"

        except ValueError as e:
            return f"{e}"


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        val: bool = isinstance(data, str) and len(data) > 0
        return val

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid text data")

            len_chars: int = len(data)
            words: int = len(data.split())

            return (f"Processed text: {len_chars} characters, {words} words")

        except ValueError as e:
            return f"{e}"


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        val: bool = isinstance(data, str) and ":" in data
        return val

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Invalid log data")

            if ":" in data:
                level, message = data.split(":", 1)
                level = level.strip().upper()
                message: str = message.strip()
                head: str = "ALERT" if "ERROR" == level else level
                return f"[{head}] {level} level detected: {message}"

            raise ValueError("Invalid log data")

        except ValueError as e:
            return f"{e}"


def polymorphic_demo(mng_list: list[DataProcessor]) -> None:
    i: int = 1
    data_list: list[Any] = [
        [1, 2, 3],
        "Hello World!",
        "INFO: System ready"
    ]
    print(
        "\n=== Polymorphic Processing Demo ===\n"
        "\nProcessing multiple data types through same interface..."
    )
    for mng, data in zip(mng_list, data_list):
        print(f"Result {i}:", mng.process(data))
        i += 1


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    # Numeric processor
    print("Initializing Numeric Processor...")
    numeric_mng: NumericProcessor = NumericProcessor()
    numeric_data: list = [1, 2, 3, 4, 5]

    print("Processing data:", numeric_data)
    if numeric_mng.validate(numeric_data):
        print("Validation: Numeric data verified")
    print(numeric_mng.format_output(numeric_mng.process(numeric_data)))

    # Text processor
    print("\nInitializing Text Processor...")
    text_mng: TextProcessor = TextProcessor()
    text_data: str = "Hello Nexus World"

    print(f'Processing data: "{text_data}"')
    if text_mng.validate(text_data):
        print("Validation: Text data verified")
    print(text_mng.format_output(text_mng.process(text_data)))

    # Log processor
    print("\nInitializing Log Processor...")
    log_mng: LogProcessor = LogProcessor()
    log_data: str = "ERROR: Connection timeout"

    print(f'Processing data: "{log_data}"')
    if log_mng.validate(log_data):
        print("Validation: Log entry verified")
    print(log_mng.format_output(log_mng.process(log_data)))

    mng_list: list[DataProcessor] = [numeric_mng, text_mng, log_mng]
    polymorphic_demo(mng_list)

    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
