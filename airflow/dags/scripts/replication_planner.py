import math

class ReplicationPlanner:
    def __init__(self, extractor, store, chunk_size: int = 500) -> None:
        self.chunk_size = chunk_size
        self.extractor = extractor
        self.store = store

    def plan(self, pipeline_name: str, collection_name, watermark_field: str) -> dict:
        watermark = self.store.get_watermark(pipeline_name)
        new_entries = self.extractor.count_new_entries(collection_name, watermark_field, watermark)
        saved_watermark_value = self.extractor.fetch_max_value(collection_name, watermark_field, watermark)
        max_watermark = saved_watermark_value if saved_watermark_value is not None else 0
        chunks = math.ceil(new_entries / self.chunk_size) if new_entries > 0 else 0
        manifest = {
            "pipeline_name": pipeline_name,
            "collection_name": collection_name,
            "watermark_field": watermark_field,
            "watermark": watermark,
            "max_watermark": max_watermark,
            "new_entries": new_entries,
            "chunk_size": self.chunk_size,
            "chunks": chunks,
        }

        # Сохраним манифест на диск.
        self.store.set_manifest(pipeline_name, manifest)
        return manifest
