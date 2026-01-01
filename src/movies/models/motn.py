from pgvector.django import VectorField

from django.conf import settings
from django.contrib.postgres.fields.array import ArrayField
from django.db import models


class MotnShow(models.Model):
    """
    Show object as returned by Movie of the Night / Streaming Availability API.

    This model stores key scalar fields explicitly and keeps the full upstream
    structure (including future fields) in `raw`.
    """

    # Identifier used by the Streaming Availability API (IMDb/TMDb/own id)
    motn_id = models.CharField(
        max_length=64,
        unique=True,
        help_text="Show identifier as used by Streaming Availability API "
                  "(often IMDb or TMDb id).",
    )
    source_id = models.BigIntegerField(
        unique=True,
        null=True,
        blank=True,
        help_text="Identifier of the external streaming source (e.g. Netflix)",
    )

    # Basic metadata
    title = models.CharField(max_length=512)
    original_title = models.CharField(max_length=512, blank=True)
    overview = models.TextField(blank=True)

    show_type = models.CharField(
        max_length=16,
        blank=True,
        help_text="Type of show, e.g. 'movie' or 'series'.",
    )
    year = models.PositiveIntegerField(null=True, blank=True)  # TODO firstAirYear, lastAirYear
    runtime = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Runtime in minutes.",
    )
    age_certification = models.CharField(
        max_length=16,
        blank=True,
        help_text="Age rating / certification (e.g. 'PG-13', 'TV-MA').",
    )
    season_count = models.SmallIntegerField(null=True, blank=True)
    episode_count = models.SmallIntegerField(null=True, blank=True)

    # IDs and ratings
    imdb_id = models.CharField(max_length=32, blank=True)
    imdb_rating = models.DecimalField(
        max_digits=4, decimal_places=2, null=True, blank=True
    )
    imdb_vote_count = models.IntegerField(null=True, blank=True)

    tmdb_id = models.IntegerField(null=True, blank=True)
    tmdb_rating = models.DecimalField(
        max_digits=4, decimal_places=2, null=True, blank=True
    )

    # Localization / taxonomy
    original_language = models.CharField(max_length=8, blank=True)
    genres = models.ManyToManyField(
        "MotnGenre",
        through="MotnShowGenre",
        related_name="shows",
        blank=True,
        help_text="List of genres as returned by the API.",
    )
    cast = models.JSONField(
        default=list,
        blank=True,
        help_text="List of cast members.",
    )
    directors = models.JSONField(
        default=list,
        blank=True,
        help_text="List of directors.",
    )
    countries = models.JSONField(
        default=list,
        blank=True,
        help_text="Production / availability countries.",
    )
    tags = models.JSONField(
        default=list,
        blank=True,
        help_text="Additional tags/keywords from the API, if any.",
    )

    # Images
    poster_urls = models.JSONField(
        default=dict,
        blank=True,
        help_text="Dict of poster URLs keyed by size (e.g. '92', '185', 'original').",
    )
    backdrop_urls = models.JSONField(
        default=dict,
        blank=True,
        help_text="Dict of backdrop URLs keyed by size.",
    )

    # Streaming info
    streaming_options = models.JSONField(
        default=dict,
        blank=True,
        help_text="Map of country code -> list of streaming options.",
    )

    # Generated

    embedding = VectorField(dimensions=settings.OPENAI_EMBEDDING_DIM, null=True, blank=True)
    # plot_embedding = VectorField(dimensions=settings.OPENAI_EMBEDDING_DIM, null=True, blank=True)
    # meta_embedding = VectorField(dimensions=settings.OPENAI_EMBEDDING_DIM, null=True, blank=True)
    # tone_embedding = VectorField(dimensions=settings.OPENAI_EMBEDDING_DIM, null=True, blank=True)

    relevant_queries = ArrayField(models.CharField(max_length=120, blank=True), null=True)

    # Bookkeeping
    added_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Movie of the Night show"
        verbose_name_plural = "Movie of the Night shows"
        indexes = [
            models.Index(fields=["motn_id"]),
            models.Index(fields=["imdb_id"]),
            models.Index(fields=["tmdb_id"]),
            models.Index(fields=["show_type", "year"]),
        ]

    def __str__(self) -> str:
        return f"{self.title} ({self.year or 'n/a'})"

    def __repr__(self):
        return f"<{self.id}: {self}>"

    def _normalize_list_field(self, value):
        """
        Handle JSONField that may be a list of strings or list of dicts with 'name'.
        """
        if not value:
            return []

        items = []
        for item in value:
            if isinstance(item, str):
                items.append(item)
            elif isinstance(item, dict):
                # Try a few common keys; adjust to your actual API shape
                for key in ("name", "title", "full_name"):
                    if key in item and item[key]:
                        items.append(str(item[key]))
                        break
        return items

    @property
    def embedding_text(self) -> str:
        """
        Canonical text representation used to build embeddings.
        """
        parts = []

        # Title + year + type
        if self.year:
            parts.append(f"{self.title} ({self.year}) - {self.show_type or 'series'}")
        else:
            parts.append(f"{self.title} - {self.show_type or 'series'}")

        if self.original_title:
            parts.append(f"Also known as: {self.original_title}")

        # Genres
        genre_names = [g.name for g in self.genres.all()] if hasattr(self, "genres") else []
        if genre_names:
            parts.append("Genres: " + ", ".join([str(g) for g in genre_names]))

        # Countries / language if you care
        if self.countries:
            parts.append("Countries: " + ", ".join([str(c) for c in self.countries]))
        if self.original_language:
            parts.append(f"Language: {self.original_language}")
        if self.age_certification:
            parts.append(f"Age rating: {self.age_certification}")

        # Plot (main semantic signal)
        if self.overview:
            parts.append("Plot: " + self.overview)

        return ". ".join(parts) + "."
        #return self.overview


class MotnGenre(models.Model):
    name = models.CharField(max_length=100, unique=True)

    def __str__(self) -> str:
        return self.name


class MotnShowGenre(models.Model):
    show = models.ForeignKey(MotnShow, on_delete=models.CASCADE)
    genre = models.ForeignKey(MotnGenre, on_delete=models.CASCADE)

    class Meta:
        unique_together = ("show", "genre")
