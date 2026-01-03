#!/usr/bin/env python3
"""
Import MLflow baseline experiments from the baseline directory.

This script uses MLflow's Python API directly for better compatibility with SQLite backends.
It is typically run during project setup to load shared baseline experiments into your local MLflow database.

Usage:
    # Import all baseline experiments
    uv run python benchmark/import_baseline.py

    # Import from a specific directory
    uv run python benchmark/import_baseline.py --input-dir benchmark/baseline/custom
"""

import argparse
import json
import os
from pathlib import Path

import mlflow

# MLflow tracking URI for the benchmark database
MLFLOW_TRACKING_URI = "sqlite:///mlflow.db"
BASELINE_DIR = Path(__file__).parent / "baseline"


def import_run(run_dir: Path, experiment_id: str):
    """Import a single run from JSON format."""
    run_json_path = run_dir / "run.json"

    if not run_json_path.exists():
        return False

    with open(run_json_path) as f:
        run_data = json.load(f)

    client = mlflow.MlflowClient()

    # Create a new run
    run = client.create_run(experiment_id)

    # Log parameters
    for key, value in run_data["data"]["params"].items():
        client.log_param(run.info.run_id, key, value)

    # Log metrics
    for key, value in run_data["data"]["metrics"].items():
        client.log_metric(run.info.run_id, key, value)

    # Log tags
    for key, value in run_data["data"]["tags"].items():
        client.set_tag(run.info.run_id, key, value)

    # Try to log artifacts
    artifacts_dir = run_dir / "artifacts"
    if artifacts_dir.exists() and any(artifacts_dir.iterdir()):
        try:
            client.log_artifacts(run.info.run_id, str(artifacts_dir))
        except Exception as e:
            print(f"    ⚠️  Could not import artifacts: {e}")

    # End the run
    client.set_terminated(run.info.run_id, status="FINISHED")

    return True


def import_experiment(exp_dir: Path):
    """Import an experiment from the baseline directory."""
    exp_json_path = exp_dir / "experiment.json"

    if not exp_json_path.exists():
        print(f"  ⚠️  No experiment.json found in {exp_dir.name}, skipping")
        return False

    with open(exp_json_path) as f:
        exp_data = json.load(f)

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = mlflow.MlflowClient()

    # Check if experiment already exists
    existing_exp = client.get_experiment_by_name(exp_data["name"])
    if existing_exp:
        print(f"  ℹ️  Experiment '{exp_data['name']}' already exists, using existing")
        experiment_id = existing_exp.experiment_id
    else:
        # Create new experiment
        experiment_id = client.create_experiment(
            exp_data["name"],
            tags=exp_data.get("tags", {})
        )
        print(f"  ✓ Created experiment '{exp_data['name']}'")

    # Import runs
    runs_dir = exp_dir / "runs"
    if not runs_dir.exists():
        print(f"  ⚠️  No runs directory found in {exp_dir.name}")
        return True

    run_dirs = [d for d in runs_dir.iterdir() if d.is_dir()]
    imported_count = 0

    for run_dir in run_dirs:
        try:
            if import_run(run_dir, experiment_id):
                imported_count += 1
                print(f"    ✓ Imported run {run_dir.name[:8]} ({imported_count}/{len(run_dirs)})")
        except Exception as e:
            print(f"    ⚠️  Failed to import run {run_dir.name[:8]}: {e}")

    print(f"  ✓ Imported {imported_count}/{len(run_dirs)} runs")
    return True


def import_all_experiments(input_dir: Path):
    """Import all experiments from the baseline directory."""
    if not input_dir.exists():
        print(f"⚠️  Baseline directory not found: {input_dir}")
        print("No baselines to import. This is normal for a fresh clone.")
        return

    # Check for actual experiment exports (directories with experiment.json)
    experiment_dirs = [
        d for d in input_dir.iterdir()
        if d.is_dir() and not d.name.startswith('.') and (d / "experiment.json").exists()
    ]

    if not experiment_dirs:
        print(f"⚠️  No baseline experiments found in: {input_dir}")
        print("This is normal for a fresh project. Run some experiments and export them!")
        return

    print(f"Importing {len(experiment_dirs)} experiment(s) from {input_dir}...\n")

    imported_count = 0
    for exp_dir in experiment_dirs:
        print(f"Importing experiment from {exp_dir.name}...")
        try:
            if import_experiment(exp_dir):
                imported_count += 1
        except Exception as e:
            print(f"  ❌ Failed to import {exp_dir.name}: {e}")

    print(f"\n✅ Successfully imported {imported_count}/{len(experiment_dirs)} experiments!")


def main():
    parser = argparse.ArgumentParser(
        description="Import MLflow baseline experiments for local development"
    )

    parser.add_argument(
        "--input-dir",
        type=Path,
        default=BASELINE_DIR,
        help=f"Input directory containing exported experiments (default: {BASELINE_DIR})"
    )

    args = parser.parse_args()

    # Change to benchmark directory to ensure correct tracking URI resolution
    os.chdir(Path(__file__).parent)

    import_all_experiments(args.input_dir)

    print("\n✅ Import complete! View results at: http://localhost:5000")
    print(f"   (Make sure MLflow UI is running: mlflow ui --backend-store-uri {MLFLOW_TRACKING_URI} --port 5000)")


if __name__ == "__main__":
    main()
