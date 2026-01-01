from typing import Iterable

from sentence_transformers import SentenceTransformer
from django.core.management.base import BaseCommand, CommandParser
from openai import OpenAI
from movies.models import MotnShow
from core.settings import env
from django.conf import settings

class Command(BaseCommand):
    help = "Compute embeddings for titles without embeddings"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--backend",
            choices=["sentence-transformer", "openai"],
            default="openai",
            help="Embedding backend to use.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Limit number of records to embed.",
        )
        parser.add_argument(
            "--offset",
            type=int,
            default=0,
            help="Offset for batching through the queryset.",
        )

    def handle(self, *args, **options):
        backend = options["backend"]
        limit = options["limit"]
        offset = options["offset"]

        qs = MotnShow.objects.exclude(overview='')[offset:]
        if limit is not None:
            qs = qs[:limit]

        total = qs.count()
        self.stdout.write(f"Computing embeddings for {total} titles using backend={backend}")

        if backend == "openai":
            embed_fn = self._embed_with_openai
            batch_size = 1000
        else:
            embed_fn = self._embed_with_sentence_transformer
            batch_size = 256

        for start in range(0, total, batch_size):
            batch = list(qs[start:start + batch_size])
            texts = [t.embedding_text for t in batch]

            embs = embed_fn(texts)

            for obj, emb in zip(batch, embs):
                obj.embedding = emb
                obj.save(update_fields=["embedding"])

            self.stdout.write(f"Processed {start + len(batch)}/{total}")

    def _embed_with_sentence_transformer(self, texts: Iterable[str]):
        if not hasattr(self, "_st_model"):
            self._st_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2", device="cpu")

        embeddings = self._st_model.encode(list(texts), normalize_embeddings=True)
        padded = []
        for emb in embeddings:
            emb_list = emb.tolist()
            # pad to target dimension for storage compatibility
            if len(emb_list) < settings.OPENAI_EMBEDDING_DIM:
                emb_list = emb_list + [0.0] * (settings.OPENAI_EMBEDDING_DIM - len(emb_list))
            padded.append(emb_list)
        return padded

    def _embed_with_openai(self, texts: Iterable[str]):
        client = OpenAI(api_key=env("OPENAI_API_KEY"))
        response = client.embeddings.create(
            model=settings.OPENAI_EMBEDDING_MODEL,
            input=list(texts),
        )
        return [item.embedding for item in response.data]
