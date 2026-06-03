from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[tuple[int, str]] = []
        self._total_processed = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def _store(self, value: str) -> None:
        self._data.append((self._total_processed, str(value)))
        self._total_processed += 1

    def output(self) -> tuple[int, str]:
        return self._data.pop(0)


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


def main() -> None:
    print("=== Code Nexus - Data Processor ===\n")

    print("Testing Numeric Processor...")
    num_proc = NumericProcessor()
    print(f" Trying to validate input '42': {num_proc.validate(42)}")
    print(f" Trying to validate input 'Hello': {num_proc.validate('Hello')}")
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num_proc.ingest("foo")
    except ValueError as e:
        print(f" Got exception: {e}")
    print(" Processing data: [1, 2, 3, 4, 5]")
    num_proc.ingest([1, 2, 3, 4, 5])
    print(" Extracting 3 values...")
    for _ in range(3):
        output = num_proc.output()
        print(f" Numeric value {output[0]}: {output[1]}")

    print("\nTesting Text Processor...")
    text_proc = TextProcessor()
    print(f" Trying to validate input '42': {text_proc.validate(42)}")
    print(" Processing data: ['Hello', 'Nexus', 'World']")
    text_proc.ingest(["Hello", "Nexus", "World"])
    print(" Extracting 1 value...")
    rank, value = text_proc.output()
    print(f" Text value {rank}: {value}")

    print("\nTesting Log Processor...")
    log_proc = LogProcessor()
    print(f" Trying to validate input 'Hello': {log_proc.validate('Hello')}")
    logs = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"},
    ]
    print(f" Processing data: {logs}")
    log_proc.ingest(logs)
    print(" Extracting 2 values...")
    for _ in range(2):
        output = log_proc.output()
        print(f" Log entry {output[0]}: {output[1]}")


if __name__ == "__main__":
    main()
