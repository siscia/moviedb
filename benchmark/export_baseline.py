#!/usr/bin/env python3
"""
Export MLflow experiments to the baseline directory for sharing with collaborators.

This script uses MLflow's Python API directly for better compatibility with SQLite backends.

Usage:
    # Export a specific experiment by name
    uv run python benchmark/export_baseline.py --experiment "My Experiment"
    
    # Export a specific run by ID
    uv run python benchmark/export_baseline.py --run-id abc123def456
"""

import argparse
import json
import os
from pathlib import Path
from typing import Optional

import mlflow
from mlflow.entities import ViewType

# MLflow tracking URI for the benchmark database
MLFLOW_TRACKING_URI = "sqlite:///mlflow.db"
BASELINE_DIR = Path(__file__).parent / "baseline"


def export_run(run_id: str, output_dir: Path, experiment_name: Optional[str] = None):
    """Export a specific run to JSON format."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    client = mlflow.MlflowClient()
    run = client.get_run(run_id)

    # Create output directory for this run
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # Export run metadata
    run_data = {
        "info": {
            "run_id": run.info.run_id,
            "experiment_id": run.info.experiment_id,
            "status": run.info.status,
            "start_time": run.info.start_time,
            "end_time": run.info.end_time,
            "artifact_uri": run.info.artifact_uri,
        },
        "data": {
            "metrics": run.data.metrics,
            "params": run.data.params,
            "tags": run.data.tags,
        },
    }

    # Save run metadata
    with open(run_dir / "run.json", "w") as f:
        json.dump(run_data, f, indent=2)

    # Try to copy artifacts (may fail if artifacts use mlflow-artifacts:// URIs)
    try:
        artifacts_dir = run_dir / "artifacts"
        artifacts_dir.mkdir(exist_ok=True)

        # Download artifacts from MLflow
        client.download_artifacts(run_id, "", dst_path=str(artifacts_dir))
        print(f"  ✓ Exported artifacts for run {run_id[:8]}")
    except Exception as e:
        print(f"  ⚠️  Could not export artifacts for run {run_id[:8]}: {e}")

    return run_data


def export_experiment(experiment_name: str, output_dir: Path):
    """Export an entire experiment to the baseline directory."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    client = mlflow.MlflowClient()

    # Get experiment
    experiment = client.get_experiment_by_name(experiment_name)
    if not experiment:
        print(f"❌ Experiment '{experiment_name}' not found")
        return

    print(f"Exporting experiment '{experiment_name}' (ID: {experiment.experiment_id})...")

    # Create experiment directory
    exp_dir = output_dir / experiment_name.replace(" ", "_").replace("/", "_")
    exp_dir.mkdir(parents=True, exist_ok=True)

    # Export experiment metadata
    exp_data = {
        "experiment_id": experiment.experiment_id,
        "name": experiment.name,
        "artifact_location": experiment.artifact_location,
        "lifecycle_stage": experiment.lifecycle_stage,
        "tags": experiment.tags,
    }

    with open(exp_dir / "experiment.json", "w") as f:
        json.dump(exp_data, f, indent=2)

    # Get all runs for this experiment
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string="",
        run_view_type=ViewType.ACTIVE_ONLY,
    )

    print(f"Found {len(runs)} runs to export...")

    # Export each run
    runs_dir = exp_dir / "runs"
    runs_dir.mkdir(exist_ok=True)

    exported_count = 0
    for run in runs:
        try:
            export_run(run.info.run_id, runs_dir, experiment_name)
            exported_count += 1
            print(f"  ✓ Exported run {run.info.run_id[:8]} ({exported_count}/{len(runs)})")
        except Exception as e:
            print(f"  ⚠️  Failed to export run {run.info.run_id[:8]}: {e}")

    print(f"\n✓ Successfully exported {exported_count}/{len(runs)} runs for experiment '{experiment_name}'")
    return exp_dir


def main():
    parser = argparse.ArgumentParser(
        description="Export MLflow experiments/runs to baseline directory for sharing"
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--experiment",
        help="Name of the experiment to export"
    )
    group.add_argument(
        "--run-id",
        help="ID of the specific run to export"
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=BASELINE_DIR,
        help=f"Output directory (default: {BASELINE_DIR})"
    )

    args = parser.parse_args()

    # Create output directory if it doesn't exist
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Change to benchmark directory to ensure correct tracking URI resolution
    os.chdir(Path(__file__).parent)

    if args.experiment:
        exp_dir = export_experiment(args.experiment, args.output_dir)
        if exp_dir:
            print(f"\n📦 Experiment exported to: {exp_dir.absolute()}")
    elif args.run_id:
        export_run(args.run_id, args.output_dir)
        print(f"\n📦 Run exported to: {args.output_dir.absolute() / args.run_id}")

    print("\n✅ Export complete! You can now commit this directory to Git for collaborators to use.")
    print(f"   git add {args.output_dir}")
    print("   git commit -m 'Add baseline: <description>'")


if __name__ == "__main__":
    main()
