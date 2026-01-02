import numpy as np
from movies.models import UserViewInteraction

# Rating constants for weight calculation
RATING_WAY_UP = 2
RATING_UP = 1
RATING_DOWN = 0


def calculate_user_embedding(interactions_data):
    """
    interactions_data: list of objects/dicts with:
       - .show.embedding (or ['show']['embedding'])
       - .rating         (or ['rating'])
    """
    if not interactions_data:
        return None

    embs = []
    weights = []

    for inter in interactions_data:
        # Handle both object attribute or dict access (flexible for tests)
        if isinstance(inter, dict):
            # For dict, we assume structure like {'show': {'embedding': ...}, 'rating': ...}
            rating = inter.get('rating')
            show_emb = inter.get('show', {}).get('embedding')
        else:
            rating = inter.rating
            show_emb = inter.show.embedding
        
        if show_emb is None:
            continue

        emb = np.array(show_emb, dtype=float)
        
        # Define a simple weight:
        if rating == RATING_WAY_UP:
            w = 3.0
        elif rating == RATING_UP:
            w = 2.0
        elif rating == RATING_DOWN:
            w = 0.2
        else:
            w = 1.0

        embs.append(emb)
        weights.append(w)

    if not embs:
        return None

    embs = np.stack(embs)            # shape (n, d)
    weights = np.array(weights)      # shape (n,)

    user_vec = np.average(embs, axis=0, weights=weights)
    # normalize
    norm = np.linalg.norm(user_vec)
    if norm == 0:
        return None
    user_vec = user_vec / norm

    return user_vec.tolist()


def get_user_embedding(user_id: int, min_items: int = 3):
    interactions = (
        UserViewInteraction.objects
        .filter(user_id=user_id, show__embedding__isnull=False)
        .select_related("show")
    )

    if interactions.count() < min_items:
        return None  # not enough data – fall back to query-only
        
    return calculate_user_embedding(interactions)


def combine_query_and_user(q_vec, u_vec, alpha: float = 0.5):
    q = np.array(q_vec, dtype=float)
    u = np.array(u_vec, dtype=float)

    combo = alpha * q + (1 - alpha) * u
    norm = np.linalg.norm(combo)
    if norm == 0:
        return q_vec
    combo = combo / norm
    return combo.tolist()

