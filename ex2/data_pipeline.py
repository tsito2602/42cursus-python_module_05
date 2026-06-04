from typing import Any, Protocol
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[tuple[int, str]] = []
        self._total_processed = 0

    @abstractmethod
    def validate(self, data: Any) -> bool: ...

    @abstractmethod
    def ingest(self, data: Any) -> None: ...

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

    def _find_processor(self, value: list[Any]) -> DataProcessor | None:
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
        print("\n== DataStream statistics ==")
        if len(self._procs) == 0:
            print("No processor found, no data")
        for proc in self._procs:
            print(
                f"{proc.__class__.__name__}: total {proc.total_processed()} "
                f"items processed, {proc.remaining()} on processor"
            )

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._procs:
            sent = nb if nb < proc.remaining() else proc.remaining()
            data = [proc.output() for _ in range(sent)]
            plugin.process_output(data)


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None: ...


class CsvExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        output = ",".join(item[1] for item in data)
        print("CSV Output:")
        print(output)


class JsonExportPlugin:
    def _escape(self, value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"')

    def process_output(self, data: list[tuple[int, str]]) -> None:
        output = (
            "{"
            + ", ".join(
                f'"item_{rank}": "{self._escape(value)}"'
                for rank, value in data
            )
            + "}"
        )
        print("JSON Output:")
        print(output)


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===\n")

    print("Initialize Data Stream...")
    data_stream = DataStream()
    data_stream.print_processors_stats()

    print("\nRegistering Processors\n")
    data_stream.register_processor(NumericProcessor())
    data_stream.register_processor(TextProcessor())
    data_stream.register_processor(LogProcessor())

    data1 = [
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
    print(f"Send first batch of data on stream: {data1}")
    data_stream.process_stream(data1)
    data_stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    data_stream.output_pipeline(3, CsvExportPlugin())
    data_stream.print_processors_stats()

    data2 = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {"log_level": "ERROR", "log_message": "500 server crash"},
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days",
            },
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]
    print(f"\nSend another batch of data: {data2}\n")
    data_stream.process_stream(data2)
    data_stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    data_stream.output_pipeline(5, JsonExportPlugin())
    data_stream.print_processors_stats()


if __name__ == "__main__":
    main()
