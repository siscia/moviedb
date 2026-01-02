import json

import mlflow
from core.settings import env
from misc.utils.embedding import combine_query_and_user, get_user_embedding
from openai import OpenAI
from pgvector.django import CosineDistance

from django.conf import settings

from .models import MotnGenre, MotnShow

# TODO
mlflow.set_tracking_uri("http://localhost:5000")


SYSTEM_PROMPT = """
You are a query parser for a movie/series recommender.

You receive a short English request and output **only one JSON object**:

{
  "intent": "find_tv_series" | "find_movie" | "find_any",
  "must_genres": [string],
  "should_genres": [string],
  "exclude_genres": [string],
  "must_be_series": boolean,
  "must_be_movie": boolean,
  "min_year": number | null,
  "max_year": number | null,
  "tone": [string],
  "keywords": [string],
  "embedding_query_text": string
}

Constraints:
- Only use genres listed inside <available_genres> for must_genres, should_genres, exclude_genres.
- Infer preference for series/movie:
  - explicit request → set must_be_series or must_be_movie true.
  - no preference → both false.
- "intent":
  - explicit series → "find_tv_series"
  - explicit movie → "find_movie"
  - unclear → "find_any"
- Infer must_genres and should_genres from wording.
- Infer exclude_genres from implicit conflicts.
- Detect tone (e.g., dark, gritty, violent, comedic) and include in "tone".
- Extract non-genre topical keywords to "keywords".
- Extract year constraints if given; else null.
- embedding_query_text: short natural-language summary including format (movie/series) and tone if relevant.

Output rules:
- Only valid JSON.
- No text outside the JSON.
- No genres outside those in <available_genres>.


"""


def get_openai_client():
    return OpenAI(api_key=env("OPENAI_API_KEY"))


def embed_text(text: str):
    client = get_openai_client()
    response = client.embeddings.create(model=settings.OPENAI_EMBEDDING_MODEL, input=[text])
    return response.data[0].embedding


def parse_user_query(raw_query: str) -> dict:
    client = get_openai_client()
    available_genres = ",".join([x.name for x in MotnGenre.objects.all().order_by("name")])
    prompt = SYSTEM_PROMPT + f"<available_genres>{available_genres}</available_genres>"

    model = "gpt-5-nano"
    # mlflow.log_model_params({
    #     "prompt_template": prompt,
    #     "llm": model,
    # })

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": raw_query},
        ],
        response_format={"type": "json_object"},
    )
    content = resp.choices[0].message.content
    return json.loads(content)


def _clean_genre_list(genres):
    cleaned = []
    for g in genres or []:
        if g is None:
            continue
        name = str(g).strip()
        if name:
            cleaned.append(name)
    return cleaned


def build_base_queryset(structured: dict):
    qs = MotnShow.objects.all()

    # # must_be_series / must_be_movie
    # if structured.get("must_be_series"):
    #     qs = qs.filter(show_type__iexact="series")
    # elif structured.get("must_be_movie"):
    #     qs = qs.filter(show_type__iexact="movie")
    #
    # # hard genre includes/excludes (MotnGenre M2M)
    # for genre in _clean_genre_list(structured.get("must_genres")):
    #     qs = qs.filter(genres__name__iexact=genre)
    #
    # # should_genres = _clean_genre_list(structured.get("should_genres"))
    # # if should_genres:
    # #     qs = qs.filter(genres__name__in=should_genres)
    #
    # for genre in _clean_genre_list(structured.get("exclude_genres")):
    #     qs = qs.exclude(genres__name__iexact=genre)
    #
    # # optional: min_year / max_year
    # min_year = structured.get("min_year")
    # max_year = structured.get("max_year")
    # if min_year:
    #     qs = qs.filter(year__gte=min_year)
    # if max_year:
    #     qs = qs.filter(year__lte=max_year)
    return qs.distinct().prefetch_related("genres")


@mlflow.trace
def search_shows(raw_query: str, top_k: int = 20, user=None, alpha: float = 0.5, user_embedding=None):
    #structured = parse_user_query(raw_query)
    #embedding_query_text = structured.get("embedding_query_text") or raw_query
    embedding_query_text = raw_query
    structured = {}

    # embed the structured query text
    q_vec = embed_text(embedding_query_text)

    u_vec = user_embedding
    if u_vec is None and user is not None:
        u_vec = get_user_embedding(user)

    if u_vec is not None:
        q_vec = combine_query_and_user(q_vec, u_vec, alpha=alpha)

    base_qs = build_base_queryset(structured)

    # Use q_vec (combined or just query) for the distance search
    qs = (
        base_qs
        .exclude(embedding__isnull=True)
        .annotate(distance=CosineDistance("embedding", q_vec))
        .order_by("distance")[:200]
    )
    
    return qs, structured


THUMBS_WAY_UP = 2
THUMBS_UP = 1
THUMBS_DOWN = 0


def compute_score(
    sim_user: float,
    sim_query: float,
    tmdb_rating: float | None,
    tmdb_vote_count: int | None,
    watched: bool,
    thumbs: int | None,
    genre_overlap: float,
) -> float:
    # Normalize / fallback
    rating = (tmdb_rating or 0.0) / 10.0      # 0..1
    #votes = tmdb_vote_count or 0
    #popularity = math.log1p(votes) / 10.0     # squash big counts

    # base from embeddings
    score = 0.5 * sim_user + 0.3 * sim_query

    # quality prior
    score += 0.1 * rating
    #score += 0.05 * popularity

    # genre alignment (0..1)
    score += 0.1 * genre_overlap

    # user history adjustments
    if watched:
        score -= 0.5  # push watched items down but not out of the list

    if thumbs is not None:
        if thumbs == THUMBS_WAY_UP:   # way up
            score += 0.4
        elif thumbs == THUMBS_UP: # up
            score += 0.2
        elif thumbs == THUMBS_DOWN: # down
            score -= 0.7  # strong penalty

    return score
