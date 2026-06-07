from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[tuple[int, str]] = []
        self._total_processed = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def _store(self, value: str) -> None:
        self._data.append((self._total_processed, str(value)))
        self._total_processed += 1

    def output(self) -> tuple[int, str]:
        return self._data.pop(0)

    def total_processed(self) -> int:
        return self._total_processed

    def remaining(self) -> int:
        return len(self._data)


class NumericProcessor(DataProcessor):
    def _is_number(self, value: Any) -> bool:
        return isinstance(value, int | float) and not isinstance(value, bool)

    def validate(self, data: Any) -> bool:
        if self._is_number(data):
            return True
        if isinstance(data, list):
            return all(self._is_number(value) for value in data)
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        if isinstance(data, list):
            for value in data:
                self._store(str(value))
        else:
            self._store(str(data))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(value, str) for value in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        if isinstance(data, list):
            for value in data:
                self._store(str(value))
        else:
            self._store(str(data))


class LogProcessor(DataProcessor):
    def _is_valid_dict(self, value: Any) -> bool:
        return (
            isinstance(value, dict)
            and isinstance(value.get("log_level"), str)
            and isinstance(value.get("log_message"), str)
            and all(
                isinstance(key, str) and isinstance(val, str)
                for key, val in value.items()
            )
        )

    def _format_log(self, log: dict[str, str]) -> str:
        return f"{log['log_level']}: {log['log_message']}"

    def validate(self, data: Any) -> bool:
        if self._is_valid_dict(data):
            return True
        if isinstance(data, list):
            return all(self._is_valid_dict(value) for value in data)
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        if isinstance(data, list):
            for value in data:
                self._store(self._format_log(value))
        else:
            self._store(self._format_log(data))


class DataStream:
    def __init__(self) -> None:
        self._procs: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._procs.append(proc)

    def _find_processor(self, value: Any) -> DataProcessor | None:
        for proc in self._procs:
            if proc.validate(value):
                return proc
        return None

    def process_stream(self, stream: list[Any]) -> None:
        for value in stream:
            proc = self._find_processor(value)
            if proc is None:
                print(
                    f"DataStream error - Can't process element in stream: "
                    f"{value}"
                )
            else:
                proc.ingest(value)

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if len(self._procs) == 0:
            print("No processor found, no data")
        for proc in self._procs:
            print(
                f"{proc.__class__.__name__}: total {proc.total_processed()} "
                f"items processed, {proc.remaining()} on processor"
            )


def main() -> None:
    print("=== Code Nexus - Data Stream ===\n")

    print("Initialize Data Stream...")
    data_stream = DataStream()
    data_stream.print_processors_stats()

    print("\nRegistering Numeric Processor\n")
    data_stream.register_processor(NumericProcessor())

    data = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {"log_level": "INFO", "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]
    print(f"Send first batch of data on stream: {data}")
    data_stream.process_stream(data)
    data_stream.print_processors_stats()

    print("\nRegistering other data processors")
    data_stream.register_processor(TextProcessor())
    data_stream.register_processor(LogProcessor())
    print("Send the same batch again")
    data_stream.process_stream(data)
    data_stream.print_processors_stats()

    print(
        "\nConsume some elements from the data processors: "
        "Numeric 3, Text 2, Log 1"
    )
    for _ in range(3):
        data_stream._procs[0].output()
    for _ in range(2):
        data_stream._procs[1].output()
    data_stream._procs[2].output()
    data_stream.print_processors_stats()


if __name__ == "__main__":
    main()
