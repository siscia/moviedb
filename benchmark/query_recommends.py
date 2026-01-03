import asyncio
import json
import os
import sys
import tempfile
import time
from pathlib import Path

import mlflow
from mlflow.genai.scorers import scorer
from openai import AsyncOpenAI

import django

mlflow.set_experiment("query_recommends")
mlflow.openai.autolog()

sys.path.append(str(Path(__file__).resolve().parent / ".." / "src"))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")
django.setup()

from movies.models import MotnShow  # noqa: E402
from movies.search import search_shows  # noqa: E402

client = AsyncOpenAI()

model = "gpt-5-nano"
system_prompt = """
You are an expert in generating user search queries for a movie/series recommender system.

TASK
Given the description of a TV series or movie, generate 5 different natural search queries that
a user might type to find such a show WITHOUT knowing its title.

Each query MUST:
- be in English
- sound like a real user search query
- focus on key themes, setting, tone, genre, or main conflict
- be 6–20 words
- be at most 120 characters
- be meaningfully different from the others (not minor paraphrases)

STRICT PROHIBITIONS
- DO NOT include the show title, year, actor names, or character names.
- DO NOT include company names, franchises, or IP.
- DO NOT use numbers of any kind (no years, no seasons, no episode counts).
- DO NOT copy phrases from the input description.
- DO NOT copy phrases or adjectives from the example; they are ONLY illustrative.
- DO NOT overuse any single adjective (for example, do not repeatedly use words like "gritty" or "epic" unless clearly justified by the description).

VOCABULARY RULES
- Choose adjectives and tone words that are clearly implied by the description.
- Vary your wording across the 5 queries.
- Avoid repeating the same descriptive word in more than 2 queries unless it appears multiple times in the description itself.

STYLE
- Write in casual, natural language a typical user would type into a search box.
- You may start with phrases like "looking for", "want a", "tv show about", but vary these across queries.
- Avoid marketing language or review-style phrasing.

OUTPUT FORMAT
Return ONLY a JSON array of 5 strings, with no additional text.

Example of STYLE ONLY (do NOT copy these exact words or adjectives):

Description:
"A gripping drama set in medieval England, following the lives of knights and royalty as they navigate political intrigue and epic battles."

Possible queries (STYLE EXAMPLE ONLY):
[
  "tv series set in medieval england about wars and royal power struggles",
  "show about knights and political drama in a medieval kingdom",
  "historical drama focusing on english royalty and brutal battles",
  "series with medieval wars, noble families, and court intrigue",
  "looking for a medieval drama with kings, knights, and warfare"
]

Again: the example is ONLY to show style. Do not reuse its wording.

Now, based on the provided description, generate a JSON array of 5 queries.
"""


async def _run_completions(show_texts, concurrency):
    semaphore = asyncio.Semaphore(concurrency)

    async def fetch(show, text):
        try:
            async with semaphore:
                resp = await client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": text},
                    ],
                )
            content = resp.choices[0].message.content
            return show, json.loads(content)
        except Exception as exc:
            return show, exc

    tasks = [asyncio.create_task(fetch(show, text)) for show, text in show_texts]
    return await asyncio.gather(*tasks)


def _run_bulk_completions(show_texts):
    if not hasattr(client, "batches"):
        raise RuntimeError("OpenAI batch API not available in installed SDK.")

    requests = []
    for show, text in show_texts:
        requests.append(
            {
                "custom_id": f"show-{show.id}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": text},
                    ],
                },
            }
        )

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".jsonl", delete=False) as fh:
            tmp_path = Path(fh.name)
            for req in requests:
                fh.write(json.dumps(req) + "\n")

        input_file = client.files.create(file=open(tmp_path, "rb"), purpose="batch")
        batch = client.batches.create(
            input_file_id=input_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )

        while batch.status in {"pending", "validating", "in_progress", "finalizing"}:
            time.sleep(5)
            batch = client.batches.retrieve(batch.id)

        if batch.status != "completed":
            raise RuntimeError(f"Batch failed with status: {batch.status}")

        output = client.files.content(batch.output_file_id)
        results = []
        for line in output.text.splitlines():
            data = json.loads(line)
            custom_id = data.get("custom_id")
            response_body = data.get("response", {}).get("body", {})
            content = (
                response_body.get("choices", [{}])[0]
                .get("message", {})
                .get("content")
            )
            if not custom_id or content is None:
                continue
            show_id = int(custom_id.replace("show-", ""))
            results.append((show_id, json.loads(content)))
        return results
    finally:
        if tmp_path and tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


@mlflow.trace
def generate_user_queries(concurrency=5, prefer_batch=False, target_count=1000):
    # Check how many shows already have relevant_queries
    existing_count = MotnShow.objects.exclude(relevant_queries=[]).count()

    if existing_count >= target_count:
        print(f"Already have {existing_count} shows with relevant_queries (target: {target_count}). Nothing to do.")
        return

    # Calculate how many more we need
    needed = target_count - existing_count
    print(f"Found {existing_count} shows with relevant_queries. Generating for {needed} more to reach {target_count}.")

    shows = list(
        MotnShow.objects.filter(relevant_queries=[]).prefetch_related("genres").order_by('?')[:needed]
    )

    if not shows:
        print("No shows available for relevant_queries generation.")
        return

    prefer_batch = prefer_batch or bool(int(os.getenv("OPENAI_USE_BATCH", "0")))

    # Compute embedding_text synchronously to avoid DB access inside asyncio tasks
    show_texts = [(show, show.embedding_text) for show in shows]

    results = None
    if prefer_batch:
        try:
            bulk_results = _run_bulk_completions(show_texts)
            show_by_id = {s.id: s for s in shows}
            mapped = []
            for show_id, payload in bulk_results:
                show = show_by_id.get(show_id)
                if not show:
                    continue
                mapped.append((show, payload))
            results = mapped
        except Exception as exc:
            print(f"Bulk completions unavailable/failing, falling back to async: {exc}")

    if results is None:
        results = asyncio.run(_run_completions(show_texts, concurrency))

    to_update = []
    for show, payload in results:
        if isinstance(payload, Exception):
            print(f"Failed to generate queries for show {show.id}: {payload}")
            continue
        show.relevant_queries = payload
        to_update.append(show)

    if to_update:
        MotnShow.objects.bulk_update(to_update, ["relevant_queries"])
        print(f"Updated {len(to_update)} shows.")
    else:
        print("No shows were updated.")


@scorer
def hit(outputs, expectations) -> bool:
    """
    True if target show is anywhere in outputs, else False.
    """
    target = expectations["target_show"]
    return target in outputs


@scorer
def rank_score(outputs, expectations) -> float:
    """
    Rank-based score:
      - 1.0 if target is first in outputs
      - 0.0 if target is last in outputs
      - no numeric score (error) if target is missing
    """
    target = expectations["target_show"]

    if not isinstance(outputs, list) or len(outputs) == 0:
        # No ranked results -> undefined rank; let MLflow record an error
        raise ValueError("rank_score: empty or invalid outputs list")

    try:
        idx = outputs.index(target)
    except ValueError:
        # Target not present -> treat as 'no score' by raising
        raise ValueError("rank_score: target show not present in outputs")

    n = len(outputs)
    if n == 1:
        return 1.0  # only one item

    # 0-based index -> [1.0 .. 0.0]
    score = 1.0 - (idx / (n - 1))
    return float(score)


def predict_fn(query: str) -> list[str]:
    qs, _ = search_shows(query, top_k=20)
    return [str(s) for s in qs]


def evaluate_search_shows(target_count=100):
    shows = list(
        MotnShow.objects.exclude(relevant_queries=[]).order_by('?')[:target_count]
    )
    eval_dataset = []
    for show in shows:
        for query in show.relevant_queries:
            eval_dataset.append({
                "inputs": {"query": query},
                "expectations": {"target_show": str(show)},
            })

    mlflow.set_tag("mlflow.runName", "evaluate_1000_random")
    mlflow.genai.evaluate(
        data=eval_dataset,
        predict_fn=predict_fn,
        scorers=[hit, rank_score],
    )


if __name__ == "__main__":
    target_count = 1000
    generate_user_queries(target_count=target_count)
    evaluate_search_shows(target_count=target_count)
