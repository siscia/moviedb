import os
import sys

import mlflow
import streamlit as st

mlflow.openai.autolog()
mlflow.set_experiment("my-genai-experiment")

@st.cache_resource
def django_setup():
    # Adjust these to point to your Django project
    DJANGO_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    if DJANGO_PROJECT_ROOT not in sys.path:
        sys.path.append(DJANGO_PROJECT_ROOT)

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")

    import django  # noqa: E402, PLC0415
    django.setup()

django_setup()

from movies.search import search_shows as _search_shows  # noqa: E402


@st.cache_resource(show_spinner=False)
def search_shows(*args, **kwargs):
    return _search_shows(*args, **kwargs)

st.set_page_config(
    page_title="MovieDB: AI-powered movie recommendations",
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
st.subheader("AI-powered Netflix NL recommendations")

query = st.text_area("Describe what you want to watch:", height=100, value="gritty medieval series where a king fights vikings")

top_k = st.slider("Number of results", min_value=5, max_value=50, value=10, step=5)

if st.button("Search") and query.strip():
    with st.spinner("Searching..."):
        results, structured = search_shows(query.strip(), top_k=top_k, user=1)  # TODO: user=1

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
                st.markdown(f"### {show.title} ({show.year or 'n/a'})")
                st.write(show.overview)

            try:
                col2.page_link(show.streaming_options['nl'][0]['videoLink'], label="Watch on Netflix", icon="▶️")
            except (KeyError, IndexError):
                pass

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
