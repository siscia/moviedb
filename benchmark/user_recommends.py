import os
import sys
from pathlib import Path
import random

import django
import mlflow
import pandas as pd
from mlflow.genai.scorers import scorer

# Set up Django environment
sys.path.append(str(Path(__file__).resolve().parent / ".." / "src"))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")
django.setup()

from movies.models import UserViewInteraction
from misc.utils.embedding import calculate_user_embedding
from movies.search import search_shows

# MLflow setup
mlflow.set_tracking_uri("sqlite:///mlflow.db")
mlflow.set_experiment("user_recommends_evaluation")


@scorer
def hit_at_10(outputs, expectations) -> bool:
    """True if target show is in the top 10 outputs."""
    target = expectations["target_show_id"]
    # search_shows returns queryset of objects, we need to check IDs
    # outputs is expected to be a list of projected results (e.g. IDs or titles)
    # The predict_fn will return a list of IDs for simplicity.
    return target in outputs[:10]


@scorer
def mrr(outputs, expectations) -> float:
    """Mean Reciprocal Rank."""
    target = expectations["target_show_id"]
    try:
        rank = outputs.index(target) + 1
        return 1.0 / rank
    except ValueError:
        return 0.0


def predict_fn(user_embedding_context):
    """
    Receives user_embedding_context.
    Can be a single embedding (list of floats) or a pandas Series (batch).
    """
    import pandas as pd
    import numpy as np

    results = []
    
    # Determine if input is a batch (Series or list of lists) or single item (list of floats)
    is_batch = False
    if isinstance(user_embedding_context, pd.Series):
        batch = user_embedding_context.tolist()
        is_batch = True
    elif isinstance(user_embedding_context, list) and len(user_embedding_context) > 0 and isinstance(user_embedding_context[0], (list, np.ndarray)):
        # List of embeddings -> batch
        batch = user_embedding_context
        is_batch = True
    else:
        # Single embedding
        batch = [user_embedding_context]

    for user_embedding in batch:
        # We search with a neutral query to rely on user embedding
        # 'recommend' or empty string could be used. 
        # The user requested 'relevant recommendations', often implies 'what should I watch?'
        qs, _ = search_shows(
            raw_query="recommend shows", 
            top_k=50, 
            user_embedding=user_embedding,
            alpha=0.2 # low alpha -> high weight on user embedding
        )
        
        # Return list of IDs (strings or ints)
        results.append([s.id for s in qs])
    
    if is_batch:
        return results
    else:
        # If input was single item, mlflow might expect single item result?
        # Usually predict_fn returns a list of size 1 if input size is 1.
        # But if it wasn't a batch call... 
        # Let's return the list, usually safe.
        return results[0] # Wait, if it expects exact return matching return type...
        # If I return result[0], it's [id1, id2...]
        # If I return results, it's [[id1, id2...]]
        # If predict_fn returns string/list, and we used list, it should be fine.
        
        # Actually, let's always return results (list of lists) if it's consistent.
        # But if `mlflow` called with `**sample_input` (one item), it captures the return.
        # If I return `[[ids]]` it might treat it as "result is list of ids".
        # If I return `[ids]` it might treat it as "result is list of ids".
        
        # Let's look at `test.py`: returns `list[str]`.
        # So for one input, it returns ONE list of strings.
        # So I should return `[id1, id2...]` for a single input.
        return results[0]


def build_evaluation_dataset(min_interactions=5):
    """
    Builds a dataset for evaluation.
    Returns a list of dicts:
    {
        "inputs": {"user_embedding_context": [...]},
        "expectations": {"target_show_id": ...}
    }
    """
    # 1. Fetch users with enough history
    users_with_history = (
        UserViewInteraction.objects
        .values("user_id")
        .annotate(count=django.db.models.Count("id"))
        .filter(count__gte=min_interactions)
    )
    user_ids = [u["user_id"] for u in users_with_history]
    print(f"Found {len(user_ids)} users with >= {min_interactions} interactions.")

    dataset = []

    for uid in user_ids:
        # fetch interactions sorted by date (if available) or id (proxy for time)
        # Using last_date or first_date if populated, fall back to id
        inters = (
            UserViewInteraction.objects
            .filter(user_id=uid, show__embedding__isnull=False)
            .select_related("show")
            .order_by("last_date", "id")
        )
        inters_list = list(inters)
        
        if len(inters_list) < min_interactions:
            continue

        # Split: Train (first 80%), Test (last 20%)
        # Or Leave-One-Out (last one as test)
        # The user's prompt suggested 80/20 split, then leave-one-out ranking for each test item.
        # Let's simple Leave-One-Out for the single most recent item, or a few recent items.
        # Let's do a 20% holdout.
        
        split_idx = int(len(inters_list) * 0.8)
        train_inters = inters_list[:split_idx]
        test_inters = inters_list[split_idx:]

        # If train set is too small to build a good embedding, skip
        if len(train_inters) < 1:
            continue

        # Calculate User Embedding from TRAIN interactions
        # We need to reshape them for our helper:
        train_data_for_calc = []
        for i in train_inters:
            train_data_for_calc.append({
                "rating": i.rating,
                "show": {"embedding": i.show.embedding}
            })
        
        user_emb = calculate_user_embedding(train_data_for_calc)
        
        if user_emb is None:
            continue

        # Add test cases
        for test_item in test_inters:
            if test_item.rating == 0: # Down
                continue

            dataset.append({
                "inputs": {"user_embedding_context": user_emb},
                "expectations": {"target_show_id": test_item.show.id},
                # Extra metadata can be top-level or in 'meta'? 
                # For now let's keep it simple.
            })

    return dataset


def run_evaluation():
    print("Building evaluation dataset...")
    data = build_evaluation_dataset()
    print(f"Generated {len(data)} evaluation samples.")
    
    if not data:
        print("No data found. Exiting.")
        return

    # Pass list of dicts directly to mlflow.genai.evaluate
    
    with mlflow.start_run(run_name="leave_one_out_eval"):
        results = mlflow.genai.evaluate(
            data=data,
            predict_fn=predict_fn,
            scorers=[hit_at_10, mrr],
        )
        
        print("\nEvaluation Results:")
        print(results.metrics)


if __name__ == "__main__":
    run_evaluation()
