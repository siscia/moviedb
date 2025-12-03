from django.contrib import admin
from admin_numeric_filter.admin import NumericFilterModelAdmin, RangeNumericFilter

from .models import ImdbGenre, ImdbMovie, ImdbTitleType, MotnShow


@admin.register(ImdbMovie)
class ImdbMovieAdmin(NumericFilterModelAdmin):
    list_display = ("imdb_id", "title", "title_type", "start_year")
    list_filter = (
        "title_type",
        "genres",
        "is_adult",
        ("start_year", RangeNumericFilter),
    )
    search_fields = ("imdb_id", "title", "original_title")
    ordering = ("imdb_id",)


@admin.register(ImdbTitleType)
class ImdbTitleTypeAdmin(admin.ModelAdmin):
    search_fields = ("name",)
    ordering = ("name",)


@admin.register(ImdbGenre)
class ImdbGenreAdmin(admin.ModelAdmin):
    search_fields = ("name",)
    ordering = ("name",)


@admin.register(MotnShow)
class MotnShowAdmin(NumericFilterModelAdmin):
    list_display = ("motn_id", "title", "show_type", "year")
    list_filter = (
        "show_type",
        ("year", RangeNumericFilter),
    )
    search_fields = ("motn_id", "title", "original_title")
