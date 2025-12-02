from django.db import models

class ImdbMovie(models.Model):
    imdb_id = models.CharField(max_length=20, unique=True)
    title = models.CharField(max_length=255)
    original_title = models.CharField(max_length=255, blank=True)
    title_type = models.ForeignKey('ImdbTitleType', on_delete=models.CASCADE)
    is_adult = models.BooleanField(default=False)
    start_year = models.IntegerField()
    end_year = models.IntegerField(null=True, blank=True)
    runtime_minutes = models.IntegerField(null=True, blank=True)
    genres = models.ForeignKey('ImdbGenre', on_delete=models.CASCADE)

    def __str__(self):
        return f"{self.title} ({self.start_year})"


class ImdbTitleType(models.Model):
    name = models.CharField(max_length=100, unique=True)

    def __str__(self):
        return self.name


class ImdbGenre(models.Model):
    name = models.CharField(max_length=100, unique=True)

    def __str__(self):
        return self.name
