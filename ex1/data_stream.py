from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union, Dict


class DataStream(ABC):
    # guarda cada stream_id en self.stream_id
    def __init__(self, stream_id: str) -> None:
        self.stream_id: str = stream_id
        self.count: int = 0

    # funciones basicas de transmisión de datos
    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data"""
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter data based on criteria"""
        if criteria is None:
            return data_batch

        new_list: List[Any] = [data for data in data_batch if criteria in data]
        return new_list

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics"""
        return {
            "stream_id": self.stream_id,
            "type": self.__class__.__name__,
            "processed_count": self.count
        }


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return data_batch

        new_list: List[Any] = [float((data.split(":")[1]))
                               for data in data_batch if criteria in data]
        return new_list

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            self.count += len(data_batch)
            n_data: int = len(data_batch)
            temps: List[float] = self.filter_data(data_batch, "temp")
            avg_temp: float = sum(temps) / len(temps) if temps else 0.0
            return (f"{n_data} readings processed, avg temp: {avg_temp}°C")
        except (ValueError, IndexError, TypeError) as e:
            return f"{e}"


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return data_batch
        new_list: List[Any] = [int((data.split(":")[1]))
                               for data in data_batch if criteria in data]
        return new_list

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            self.count += len(data_batch)
            total_buy: List[int] = self.filter_data(data_batch, "buy")
            total_sell: List[int] = self.filter_data(data_batch, "sell")
            net: int = sum(total_buy) - sum(total_sell)

            if net < 0:
                return f"{len(data_batch)} operations, net flow: {net} units"
            return f"{len(data_batch)} operations, net flow: +{net} units"
        except (ValueError, IndexError, TypeError) as e:
            return f"{e}"


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            self.count += len(data_batch)
            len_errors: int = len(self.filter_data(data_batch, "error"))
            return f"{len(data_batch)} events, {len_errors} errors detected"
        except (ValueError, IndexError, TypeError) as e:
            return f"{e}"


def stream_processing() -> None:
    print(
        "\n=== Polymorphic Stream Processing ===\n"
        "Processing mixed stream types through unified interface...\n\n"
        "Batch 1 Results:")

    mng_list: list[DataStream] = [
        SensorStream("SENSOR_002"),
        TransactionStream("TRANS_002"),
        EventStream("EVENT_002")
    ]

    batch_list: List[List[str]] = [
        ["temp:30.0", "temp:28.0"],
        ["buy:50", "sell:150", "buy:10", "sell:20"],
        ["login", "error", "logout"]
    ]

    # Process batch
    for mng, batch in zip(mng_list, batch_list):
        mng.process_batch(batch)

    # Display info
        if isinstance(mng, SensorStream):
            print(f"- Sensor data: {mng.count} readings processed")
        elif isinstance(mng, TransactionStream):
            print(f"- Transaction data: {mng.count} operations processed")
        elif isinstance(mng, EventStream):
            print(f"- Event data: {mng.count} events processed")

    filtered_results(mng_list, batch_list)


def filtered_results(mng_list: List[DataStream],
                     batch_list: List[List[str]]) -> None:
    sensor_alert: int = 0
    large_transaction: int = 0

    for mng, batch in zip(mng_list, batch_list):
        if isinstance(mng, SensorStream):
            sensor_alert += len(mng.filter_data(batch, "temp"))
        elif isinstance(mng, TransactionStream):
            large_list: List[str] = [data for data in batch
                                     if int(data.split(":")[1]) > 100]
            large_transaction = len(large_list)

    print("\nStream filtering active: High-priority data only"
          f"\nFiltered results: {sensor_alert} critical sensor alerts, "
          f"{large_transaction} large transaction")


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    # Sensor stream
    sensor_mng: SensorStream = SensorStream("SENSOR_001")
    sensor_batch: List[str] = ["temp:22.5", "humidity:65", "pressure:1013"]

    print(
        "Initializing Sensor Stream...\n"
        f"Stream ID: {sensor_mng.stream_id}, Type: Environmental Data\n"
        f"Processing sensor batch: [{', '.join(sensor_batch)}]\n"
        f"Sensor analysis: {sensor_mng.process_batch(sensor_batch)}\n")

    # Transaction Stream
    trans_mng: TransactionStream = TransactionStream("TRANS_001")
    trans_batch: List[str] = ["buy:100", "sell:150", "buy:75"]

    print(
        "Initializing Transaction Stream...\n"
        f"Stream ID: {trans_mng.stream_id}, Type: Financial Data\n"
        f"Processing transaction batch: [{', '.join(trans_batch)}]\n"
        f"Transaction analysis: {trans_mng.process_batch(trans_batch)}\n")

    # Event Stream
    event_mng: EventStream = EventStream("EVENT_001")
    event_batch: List[str] = ["login", "error", "logout"]

    print(
        "Initializing Event Stream...\n"
        f"Stream ID: {event_mng.stream_id}, Type: System Events\n"
        f"Processing event batch: [{', '.join(event_batch)}]\n"
        f"Event analysis: {event_mng.process_batch(event_batch)}")


if __name__ == "__main__":
    main()
    stream_processing()
    print("\nAll streams processed successfully. Nexus throughput optimal.")
