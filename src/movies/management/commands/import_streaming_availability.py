"""Import script for streaming availability API data, also known as Movie of the Night.
See: https://docs.movieofthenight.com/
"""

import gzip
import json
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path

import requests
from core.settings import env

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from movies.models import MotnGenre, MotnShow, MotnShowGenre

streaming_availability_filter_url = "https://streaming-availability.p.rapidapi.com/shows/search/filters"

headers = {
    "x-rapidapi-key": env("STREAMING_AVAILABILITY_API_KEY"),
    "x-rapidapi-host": "streaming-availability.p.rapidapi.com"
}

default_querystring = {
    "country": "nl",
    "series_granularity": "show",
    "order_direction": "desc",
    "order_by": "release_date",
    "catalogs": "netflix"
}


NETFLIX_ID_RE = re.compile(r'https://www\.netflix\.com/(?:title|watch)/(\d+)/?')
BATCH_SIZE = 500


class Command(BaseCommand):

    def handle(self, *args, **options):
        output_dir = settings.BASE_DIR / "data" / "motn"
        input_file = output_dir / "netflix-nl.jsonl.gz"
        created_total = self._import_from_local_file(input_file)
        self.stdout.write(self.style.SUCCESS(f"Finished import. Attempted to create {created_total} shows."))

    def _import_from_local_file(self, input_file: Path) -> int:
        if not input_file.exists():
            raise CommandError(f"Input file not found: {input_file}")

        shows_to_create: list[tuple[MotnShow, list[str]]] = []
        created_total = 0
        processed = 0

        for show in load_shows_from_file(input_file):
            motn_show, genres = to_motn_show(show)
            if motn_show:
                shows_to_create.append((motn_show, genres))

            if len(shows_to_create) >= BATCH_SIZE:
                created_total += self._flush_batch(shows_to_create)

            processed += 1
            if processed % 100 == 0:
                self.stdout.write(f"Processed {processed} shows...")

        if shows_to_create:
            created_total += self._flush_batch(shows_to_create)

        return created_total

    def download_and_process_remote(self):
        """Keeps the original download path for later reuse."""
        output_dir = settings.BASE_DIR / "data" / "motn"
        output_dir.mkdir(parents=True, exist_ok=True)

        shows_to_create: list[tuple[MotnShow, list[str]]] = []
        created_total = 0
        processed = 0

        for show in paginated_request():
            self._write_raw_json(show, output_dir)

            motn_show, genres = to_motn_show(show)
            if motn_show:
                shows_to_create.append((motn_show, genres))

            if len(shows_to_create) >= BATCH_SIZE:
                created_total += self._flush_batch(shows_to_create)

            processed += 1
            if processed % 100 == 0:
                self.stdout.write(f"Processed {processed} shows...")

        if shows_to_create:
            created_total += self._flush_batch(shows_to_create)

        return created_total

    def _flush_batch(self, batch: list[tuple[MotnShow, list[str]]]) -> int:
        if not batch:
            return 0

        shows = [item[0] for item in batch]
        genre_map = {item[0].motn_id: set(item[1]) for item in batch}
        motn_ids = [s.motn_id for s in shows]

        MotnShow.objects.bulk_create(shows, ignore_conflicts=True)
        shows_by_id = {s.motn_id: s for s in MotnShow.objects.filter(motn_id__in=motn_ids)}

        genre_names = {name for names in genre_map.values() for name in names if name}
        if genre_names:
            existing = {g.name: g for g in MotnGenre.objects.filter(name__in=genre_names)}
            to_create = [MotnGenre(name=name) for name in genre_names if name not in existing]
            if to_create:
                MotnGenre.objects.bulk_create(to_create, ignore_conflicts=True)
            genres_lookup = {g.name: g for g in MotnGenre.objects.filter(name__in=genre_names)}

            links = []
            for motn_id, names in genre_map.items():
                show_obj = shows_by_id.get(motn_id)
                if not show_obj:
                    continue
                for name in names:
                    genre_obj = genres_lookup.get(name)
                    if genre_obj:
                        links.append(MotnShowGenre(show=show_obj, genre=genre_obj))
            if links:
                MotnShowGenre.objects.bulk_create(links, ignore_conflicts=True)

        created = len(batch)
        batch.clear()
        return created

    def _write_raw_json(self, show: dict, output_dir):
        motn_id = show.get("imdbId") or show.get("id") or "unknown-id"
        title = show.get("title") or "untitled"
        filename = f"{safe_filename(motn_id)} {safe_filename(title)}.json"
        path = output_dir / filename

        with path.open("w", encoding="utf-8") as fh:
            json.dump(show, fh, ensure_ascii=False, indent=2)

        # self.stdout.write(f"Saved {path.name}")


def load_shows_from_file(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            stripped_line = line.strip()
            if not stripped_line:
                continue
            yield json.loads(stripped_line)


def paginated_request():
    querystring = default_querystring.copy()

    while True:
        response = requests.get(streaming_availability_filter_url, headers=headers, params=querystring)
        response.raise_for_status()
        json_response = response.json()
        for item in json_response.get('shows', []):
            yield item
        if json_response['hasMore']:
            querystring['cursor'] = json_response['nextCursor']
        else:
            break


def to_motn_show(show: dict) -> tuple[MotnShow | None, list[str]]:
    motn_id = show.get("id")
    if not motn_id:
        return None, []

    image_set = show.get("imageSet") or {}
    age_val = show.get("ageCertification")
    if age_val in (None, "", "\\N"):
        age_val = show.get("advisedMinimumAge")

    raw_genres = [g.get("name") if isinstance(g, dict) else g for g in (show.get("genres") or [])]
    genres = []
    for name in raw_genres:
        if not name:
            continue
        genres.append(str(name).strip())

    source_id = None
    m = NETFLIX_ID_RE.search(str(show.get("streamingOptions", "")))
    if m:
        source_id = int(m.group(1))

    return MotnShow(
        motn_id=motn_id,
        source_id=source_id,
        title=show.get("title") or "",
        original_title=show.get("originalTitle") or "",
        overview=show.get("overview") or "",
        show_type=show.get("showType") or "",
        year=parse_int(show.get("releaseYear") or show.get("firstAirYear") or show.get("year")),
        runtime=parse_int(show.get("runtime")),
        season_count=parse_int(show.get("seasonCount")),
        episode_count=parse_int(show.get("episodeCount")),
        age_certification=str(age_val) if age_val not in (None, "") else "",
        imdb_id=show.get("imdbId") or "",
        imdb_rating=parse_rating(show.get("imdbRating") or show.get("rating")),
        imdb_vote_count=parse_int(show.get("imdbVoteCount")),
        tmdb_id=parse_tmdb_id(show.get("tmdbId")),
        tmdb_rating=parse_rating(show.get("tmdbRating")),
        original_language=show.get("originalLanguage") or "",
        cast=show.get("cast") or [],
        directors=show.get("directors") or show.get("creators") or [],
        countries=show.get("countries") or show.get("productionCountries") or [],
        tags=show.get("keywords") or show.get("tags") or [],
        poster_urls=image_set.get("verticalPoster") or image_set.get("horizontalPoster") or {},
        backdrop_urls=image_set.get("horizontalBackdrop") or image_set.get("verticalBackdrop") or {},
        streaming_options=show.get("streamingOptions") or {},
    ), genres


def parse_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_tmdb_id(value):
    if not value:
        return None
    match = re.search(r"(\d+)", str(value))
    if not match:
        return None
    return parse_int(match.group(1))


MAX_RATING = 10


def parse_rating(value):
    if value is None:
        return None
    try:
        rating = Decimal(str(value))
    except Exception:
        return None
    if rating > MAX_RATING:
        rating = rating / Decimal(str(MAX_RATING))
    try:
        return rating.quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


def safe_filename(value: str) -> str:
    sanitized = re.sub(r"[^\w.-]+", " ", value)
    return " ".join(sanitized.split()).strip()
