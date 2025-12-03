import json
import os
import sys

import streamlit as st
from pgvector.django import CosineDistance
from openai import OpenAI
from django.conf import settings

from core.settings import env

# -----------------------------
# 1. Bootstrap Django
# -----------------------------

@st.cache_resource
def django_setup():
    # Adjust these to point to your Django project
    DJANGO_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    if DJANGO_PROJECT_ROOT not in sys.path:
        sys.path.append(DJANGO_PROJECT_ROOT)

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")

    import django  # noqa: E402
    django.setup()

django_setup()



@st.cache_resource
def get_models():
    # Import models lazily after Django setup to avoid Streamlit rerun issues.
    from movies.models import MotnShow, MotnGenre  # noqa: WPS433
    return MotnShow, MotnGenre


MotnShow, MotnGenre = get_models()

# -----------------------------
# 2. Load embedding model once
# -----------------------------


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


@st.cache_resource
def get_query_model():
    return OpenAI(api_key=env("OPENAI_API_KEY"))

@st.cache_resource
def get_embedding_client():
    return OpenAI(api_key=env("OPENAI_API_KEY"))


def embed_text(text: str):
    client = get_embedding_client()
    response = client.embeddings.create(model=settings.OPENAI_EMBEDDING_MODEL, input=[text])
    return response.data[0].embedding


def parse_user_query(raw_query: str) -> dict:
    client = get_query_model()
    available_genres = ",".join([x.name for x in MotnGenre.objects.all().order_by("name")])
    prompt = SYSTEM_PROMPT + f"<available_genres>{available_genres}</available_genres>"
    resp = client.chat.completions.create(
        model="gpt-5-nano",  # or similar
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": raw_query},
        ],
        response_format={"type": "json_object"},
    )
    content = resp.choices[0].message.content
    return json.loads(content)


# -----------------------------
# 3. Streamlit UI
# -----------------------------

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


def search_shows(raw_query: str, top_k: int = 20):
    structured = parse_user_query(raw_query)
    embedding_query_text = structured.get("embedding_query_text") or raw_query

    # embed the structured query text
    q_vec = embed_text(embedding_query_text)

    base_qs = build_base_queryset(structured)

    qs = (
        base_qs
        .exclude(embedding__isnull=True)
        .annotate(distance=CosineDistance("embedding", q_vec))
        .order_by("distance")[:top_k]
    )

    return qs, structured


st.set_page_config(
    page_title="Movie/Series Search",
    page_icon="🎬",
    menu_items={
        'Get Help': 'https://github.com/jurrian/moviedb',
        'Report a bug': "https://github.com/jurrian/moviedb/issues",
        'About': (
            "## MovieDB\n"
            "AI-powered movie and series recommendations.\n\n"
            "Streamlit + Django app by Jurrian Tromp.\n\n"
            "Github: [github.com/jurrian/moviedb](http://github.com/jurrian/moviedb)"
        )
    }
)

st.title("MovieDB")
st.subheader("AI-powered movie recommendations")

query = st.text_area("Describe what you want to watch:", height=100, value="gritty medieval series where a king fights vikings")

top_k = st.slider("Number of results", min_value=5, max_value=50, value=10, step=5)

if st.button("Search") and query.strip():
    with st.spinner("Searching..."):
        results, structured = search_shows(query.strip(), top_k=top_k)

        st.json(structured, expanded=False)

    if not results:
        st.warning("No results found.")
    else:
        st.subheader("Results")
        for show in results:
            col1, col2 = st.columns([1, 3])
            col1.image(show.poster_urls['w240'])


            meta_bits = []
            if show.show_type:
                meta_bits.append(show.show_type)
            if show.age_certification:
                meta_bits.append(f"Rated {show.age_certification}")
            if show.original_language:
                meta_bits.append(f"Language: {show.original_language}")
            if show.imdb_rating:
                meta_bits.append(f"IMDb {show.imdb_rating}")
            if show.tmdb_rating:
                meta_bits.append(f"TMDb {show.tmdb_rating}")

            with col2.container(height=236, gap="small"):
                st.markdown(f"### {show.original_title} ({show.year or 'n/a'})")
                st.write(show.overview)

            col2.page_link(show.streaming_options['nl'][0]['videoLink'], label="Watch on Netflix", icon="▶️")

            if meta_bits:
                col1.caption(" · ".join(str(x) for x in meta_bits))

            # Optional: show genres
            # if show.genres:
            #     if isinstance(show.genres, list):
            #         genres_display = ", ".join(map(str, show.genres))
            #     else:
            #         genres_display = str(show.genres)
            #     st.caption(f"Genres: {genres_display}")

            st.markdown("---")
