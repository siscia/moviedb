from django.db import models
from pgvector.django import VectorField

class ImdbMovie(models.Model):
    imdb_id = models.CharField(max_length=20, unique=True)
    title = models.CharField(max_length=460)
    original_title = models.CharField(max_length=460, blank=True)
    title_type = models.ForeignKey('ImdbTitleType', on_delete=models.CASCADE)
    is_adult = models.BooleanField(default=False)
    start_year = models.IntegerField()
    end_year = models.IntegerField(null=True, blank=True)
    runtime_minutes = models.IntegerField(null=True, blank=True)
    genres = models.ManyToManyField('ImdbGenre', through='ImdbMovieGenre')

    # also_known = f" - also known as {t.original_title}" if t.original_title else ""
    # text = f"{t.title} ({t.start_year}){also_known}. {t.title_type}. " \
    # f"Genres: {', '.join([x.name for x in t.genres.all()] or [])}. " \
    # f"Runtime: {t.runtime_minutes} minutes"
    embedding = VectorField(dimensions=1536, null=True, blank=True)

    def __str__(self):
        return f"{self.title} ({self.start_year})"

    class Meta:
        indexes = [
            models.Index(fields=["imdb_id"]),
        ]


class ImdbTitleType(models.Model):
    name = models.CharField(max_length=100, unique=True)

    def __str__(self):
        return self.name


class ImdbGenre(models.Model):
    name = models.CharField(max_length=100, unique=True)

    def __str__(self):
        return self.name


class ImdbMovieGenre(models.Model):
    movie = models.ForeignKey(ImdbMovie, on_delete=models.CASCADE)
    genre = models.ForeignKey(ImdbGenre, on_delete=models.CASCADE)

    class Meta:
        unique_together = ('movie', 'genre')
