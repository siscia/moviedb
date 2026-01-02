import csv
import gzip
import pathlib
import urllib.request

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from movies.models import ImdbGenre, ImdbMovie, ImdbMovieGenre, ImdbTitleType

DEFAULT_IMDB_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
DEFAULT_BATCH_SIZE = 5_000
UNKNOWN_GENRE = "Unknown"
PROGRESS_LOG_INTERVAL = 100_000
MAX_PERCENTAGE = 100
PROGRESS_STEP = 5


class Command(BaseCommand):
    """
    Import IMDb title data into the database.

    This streams the TSV (1GB / ~12M rows) and bulk-creates `ImdbMovie` rows
    in batches to keep memory usage reasonable.
    """

    help = "Import IMDb datasets into the local database."

    def add_arguments(self, parser):
        parser.add_argument(
            "--source",
            type=pathlib.Path,
            required=False,
            help="Path to the IMDb dataset (e.g., the title.basics.tsv[.gz] file).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Parse and log summary without writing to the database.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=DEFAULT_BATCH_SIZE,
            help=f"Number of rows to insert per bulk_create (default: {DEFAULT_BATCH_SIZE}).",
        )

    def handle(self, *args, **options):
        source: pathlib.Path | None = options.get("source")
        dry_run: bool = options.get("dry_run", False)
        batch_size: int = options.get("batch_size", DEFAULT_BATCH_SIZE)

        using_default_source = source is None

        if source and not source.exists():
            raise CommandError(f"Source path does not exist: {source}")

        if source is None:
            source = self._download_default_dataset()

        self.stdout.write(self.style.NOTICE("Starting IMDb import"))
        self.stdout.write(f"Source: {source}")
        if dry_run:
            self.stdout.write("Running in dry-run mode; no data will be saved.")

        try:
            total_rows, created_rows = self._process_file(source, batch_size, dry_run)
        except (EOFError, OSError, gzip.BadGzipFile):
            if not using_default_source:
                raise

            self.stdout.write(
                self.style.WARNING(
                    "Dataset read failed; re-downloading default dataset and retrying."
                )
            )
            source = self._download_default_dataset(force_download=True)
            total_rows, created_rows = self._process_file(source, batch_size, dry_run)

        self.stdout.write(self.style.SUCCESS(f"Finished IMDb import. Rows read: {total_rows}, movies created: {created_rows}"))

    def _process_file(self, source: pathlib.Path, batch_size: int, dry_run: bool) -> tuple[int, int]:
        open_fn = gzip.open if source.suffix == ".gz" else open

        title_types = {t.name: t.id for t in ImdbTitleType.objects.all()}
        genres = {g.name: g.id for g in ImdbGenre.objects.all()}

        unknown_genre_id = genres.get(UNKNOWN_GENRE) or ImdbGenre.objects.get_or_create(name=UNKNOWN_GENRE)[0].id
        genres[UNKNOWN_GENRE] = unknown_genre_id

        movies_to_create: list[ImdbMovie] = []
        movie_genres: list[tuple[str, list[int]]] = []
        total_rows = 0
        created_rows = 0

        with open_fn(source, "rt", encoding="utf-8") as fh:
            reader = csv.reader(fh, delimiter="\t")
            headers = next(reader, None)  # Skip header line
            if headers is None:
                return total_rows, created_rows

            for row in reader:
                total_rows += 1
                try:
                    movie_result = self._row_to_movie(row, title_types, genres, unknown_genre_id)
                except ValueError:
                    continue

                if movie_result:
                    movie, genre_ids = movie_result
                    movies_to_create.append(movie)
                    movie_genres.append((movie.imdb_id, genre_ids))

                if len(movies_to_create) >= batch_size:
                    if not dry_run:
                        created_rows += self._bulk_insert(movies_to_create, movie_genres)
                    movies_to_create.clear()
                    movie_genres.clear()

                if total_rows and total_rows % PROGRESS_LOG_INTERVAL == 0:
                    self.stdout.write(
                        f"Processed {total_rows} rows; movies created so far: {created_rows}"
                    )

            if movies_to_create and not dry_run:
                created_rows += self._bulk_insert(movies_to_create, movie_genres)

        return total_rows, created_rows

    def _row_to_movie(self, row: list[str], title_types: dict[str, int], genres: dict[str, int], unknown_genre_id: int) -> tuple[ImdbMovie, list[int]] | None:
        try:
            (
                tconst,
                title_type_name,
                primary_title,
                original_title,
                is_adult,
                start_year,
                end_year,
                runtime_minutes,
                genres_str,
            ) = row
        except ValueError:
            raise ValueError("Unexpected column count")

        title_type_id = self._get_title_type_id(title_type_name, title_types)
        genre_ids = self._get_genre_ids(genres_str, genres, unknown_genre_id)

        if title_type_id is None or not genre_ids:
            return None

        start_year_val = self._parse_int(start_year)
        end_year_val = self._parse_int(end_year)
        runtime_val = self._parse_int(runtime_minutes)
        is_adult_val = is_adult == "1"

        movie = ImdbMovie(
            imdb_id=tconst,
            title=primary_title,
            original_title=original_title if original_title != "\\N" else "",
            title_type_id=title_type_id,
            is_adult=is_adult_val,
            start_year=start_year_val or 0,
            end_year=end_year_val,
            runtime_minutes=runtime_val,
        )

        return movie, genre_ids

    def _get_title_type_id(self, name: str, cache: dict[str, int]) -> int | None:
        if name in cache:
            return cache[name]

        obj, _ = ImdbTitleType.objects.get_or_create(name=name)
        cache[name] = obj.id
        return obj.id

    def _get_genre_ids(self, genres_value: str, cache: dict[str, int], fallback_id: int) -> list[int]:
        if not genres_value or genres_value == "\\N":
            return [fallback_id]

        names = [name.strip() for name in genres_value.split(",") if name.strip()]
        if not names:
            return [fallback_id]

        ids: list[int] = []
        for genre in names:
            if genre in cache:
                ids.append(cache[genre])
                continue

            obj, _ = ImdbGenre.objects.get_or_create(name=genre)
            cache[genre] = obj.id
            ids.append(obj.id)

        return ids or [fallback_id]

    def _parse_int(self, value: str) -> int | None:
        if value in (None, "", "\\N"):
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def _bulk_insert(self, movies: list[ImdbMovie], movie_genres: list[tuple[str, list[int]]]) -> int:
        if not movies:
            return 0

        imdb_ids = [movie.imdb_id for movie in movies]
        existing_ids = set(
            ImdbMovie.objects.filter(imdb_id__in=imdb_ids).values_list("imdb_id", flat=True)
        )
        new_ids = [mid for mid in imdb_ids if mid not in existing_ids]

        ImdbMovie.objects.bulk_create(
            movies,
            batch_size=len(movies),
            ignore_conflicts=True,  # skip rows with existing imdb_id
        )

        movie_map = {
            m.imdb_id: m.id
            for m in ImdbMovie.objects.filter(imdb_id__in=imdb_ids)
        }

        through_rows: list[ImdbMovieGenre] = []
        for imdb_id, genre_ids in movie_genres:
            movie_id = movie_map.get(imdb_id)
            if movie_id is None:
                continue

            for genre_id in genre_ids:
                through_rows.append(
                    ImdbMovieGenre(movie_id=movie_id, genre_id=genre_id)
                )

        if through_rows:
            ImdbMovieGenre.objects.bulk_create(
                through_rows,
                batch_size=len(through_rows),
                ignore_conflicts=True,
            )

        return len(new_ids)

    def _download_default_dataset(self, force_download: bool = False) -> pathlib.Path:
        data_dir = settings.BASE_DIR / "data" / "imdb"
        data_dir.mkdir(parents=True, exist_ok=True)

        target = data_dir / pathlib.Path(DEFAULT_IMDB_URL).name
        if target.exists() and not force_download:
            if self._is_valid_gzip(target):
                self.stdout.write(
                    f"Using existing dataset at {target}"
                )
                return target

            self.stdout.write(
                self.style.WARNING(
                    "Existing dataset appears corrupted; re-downloading."
                )
            )
        elif target.exists() and force_download:
            target.unlink(missing_ok=True)

        self.stdout.write(
            f"Downloading default dataset to {target}"
        )
        try:
            self._download_with_progress(DEFAULT_IMDB_URL, target)
        except Exception as exc:
            raise CommandError(f"Failed to download dataset: {exc}") from exc

        return target

    def _is_valid_gzip(self, path: pathlib.Path) -> bool:
        try:
            with gzip.open(path, "rb") as fh:
                fh.read(1)
            return True
        except (EOFError, OSError, gzip.BadGzipFile):
            return False

    def _download_with_progress(self, url: str, target: pathlib.Path) -> None:
        def report(block_num: int, block_size: int, total_size: int) -> None:
            if total_size <= 0:
                return

            downloaded = min(block_num * block_size, total_size)
            percent = int(downloaded * MAX_PERCENTAGE / total_size)

            if percent >= MAX_PERCENTAGE or percent - report.last_percent >= PROGRESS_STEP:
                mb_done = downloaded / (1024 * 1024)
                mb_total = total_size / (1024 * 1024)
                self.stdout.write(
                    f"Download progress: {percent}% ({mb_done:.1f}MB/{mb_total:.1f}MB)"
                )
                report.last_percent = percent

        report.last_percent = -5
        urllib.request.urlretrieve(url, target, reporthook=report)
