from datetime import datetime
from pathlib import Path
import json

class WatermarkStore:
    def __init__(self, root: str = "/opt/airflow/data"):
        self.root = Path(root)

    def get_watermark(self, pipeline_name: str):
        path = self._watermark_path(pipeline_name)

        # Если файла нет, то возвращаем 0 <=> нет метки о чтении.
        if not path.exists():
            return 0

        # Если файл есть, то читаем текст с него.
        # Прочитанный текст и есть метка о времени чтения.
        value = path.read_text().strip()
        return int(value) or None

    def get_manifest(self, pipeline_name: str) -> dict | None:
        path = self._manifest_path(pipeline_name)

        # Если файла нет, то возвращаем None <=> не было планирования ETL.
        if not path.exists():
            return None

        # Если файл есть, то читаем JSON.
        # Прочитанный JSON и есть план ETL.
        with open(path, "r") as f:
            return json.load(f)

    def get_raw_chunk(self, pipeline_name: str, chunk_idx: int) -> list[dict] | None:
        path = self._raw_chunk_path(pipeline_name, chunk_idx)

        # Если файла нет, то возвращаем None <=> не было извлечения чанка.
        if not path.exists():
            return None

        # Читаем чанки из JSON, каждая линия - обработанный чанк.
        with open(path, "r") as f:
            return [json.loads(line) for line in f if line.strip()]

    def get_transformed_chunk(self, pipeline_name: str, chunk_idx: int) -> dict | None:
        path = self._transformed_chunk_path(pipeline_name, chunk_idx)

        # Если файла нет, то возвращаем None <=> не было извлечения чанка.
        if not path.exists():
            return None

        with open(path, "r") as f:
            return json.load(f)

    def set_watermark(self, pipeline_name: str, value):
        path = self._watermark_path(pipeline_name)
        path.write_text(datetime.fromisoformat(value).strftime('%s'))

    def set_manifest(self, pipeline_name: str, data: dict):
        # Записываем манифест с чанками как JSON.
        with open(self._manifest_path(pipeline_name), "w") as f:
            json.dump(data, f, indent=4, default=str)

    def set_raw_chunk(self, pipeline_name: str, chunk_idx: int, rows: list[dict]):
        # Записываем обработанный чанк в виде JSON.
        with open(self._raw_chunk_path(pipeline_name, chunk_idx), "w") as f:
            for row in rows:
                f.write(json.dumps(row, default=str))
                f.write("\n")

    def set_transformed_chunk(self, pipeline_name: str, chunk_idx: int, rows: dict):
        # Записываем обработанный чанк в виде JSON.
        with open(self._transformed_chunk_path(pipeline_name, chunk_idx), "w") as f:
            json.dump(rows, f, indent=4, default=str)

    def _pipeline_path(self, pipeline_name: str) -> Path:
        path = self.root / pipeline_name
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _raw_chunk_path(self, pipeline_name: str, chunk_idx: int) -> Path:
        return self._pipeline_path(pipeline_name) / f"raw_{chunk_idx}.json"

    def _transformed_chunk_path(self, pipeline_name: str, chunk_idx: int) -> Path:
        return self._pipeline_path(pipeline_name) / f"transformed_{chunk_idx}.json"

    def _watermark_path(self, pipeline_name: str) -> Path:
        return self._pipeline_path(pipeline_name) / "watermark.txt"

    def _manifest_path(self, pipeline_name: str) -> Path:
        return self._pipeline_path(pipeline_name) / "manifest.json"
