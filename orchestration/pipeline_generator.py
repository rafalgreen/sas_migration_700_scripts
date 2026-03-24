"""Generate Step Functions state machine definitions from SAS dependency graphs.

Takes the dependency graph produced by the analyzer and generates
AWS Step Functions ASL (Amazon States Language) definitions that
orchestrate the converted Glue and Lambda jobs in the correct order.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from analyzer.dependency_graph import DependencyGraph


@dataclass
class PipelineConfig:
    pipeline_name: str
    glue_job_prefix: str = "sas-migration"
    lambda_function_prefix: str = "sas-migration"
    max_concurrent_jobs: int = 5
    job_timeout_minutes: int = 120
    lambda_timeout_seconds: int = 900
    retry_attempts: int = 1
    retry_interval_seconds: int = 60


@dataclass
class JobStep:
    """A single step in the pipeline — either a Glue job or a Lambda function."""

    name: str
    job_name: str
    target: str = "glue"
    depends_on: list[str] = field(default_factory=list)
    arguments: dict[str, str] = field(default_factory=dict)


GlueJobStep = JobStep


def _task_state_for_step(step: JobStep, config: PipelineConfig) -> dict:
    """Dispatch to the correct task state builder based on step target."""
    if step.target == "lambda":
        return _lambda_task_state(step, config)
    return _glue_task_state(step, config)


def generate_pipeline_from_graph(
    graph: DependencyGraph,
    config: PipelineConfig,
    *,
    lambda_scripts: set[str] | None = None,
) -> dict:
    """Generate a Step Functions ASL definition from a dependency graph.

    Parameters
    ----------
    lambda_scripts:
        Optional set of SAS script stems (no extension) that should
        generate Lambda invoke states instead of Glue startJobRun states.
    """
    steps = _build_steps(graph, config, lambda_scripts=lambda_scripts)
    layers = _topological_layers(steps)

    states: dict = {}
    layer_names: list[str] = []

    for i, layer in enumerate(layers):
        layer_name = f"Layer_{i}"
        layer_names.append(layer_name)

        if len(layer) == 1:
            step = layer[0]
            states[layer_name] = _task_state_for_step(step, config)
        else:
            branches = []
            for step in layer:
                branch_state = _task_state_for_step(step, config)
                branch_state.pop("Next", None)
                branch_state["End"] = True
                branches.append(
                    {
                        "StartAt": step.name,
                        "States": {step.name: branch_state},
                    }
                )
            states[layer_name] = {
                "Type": "Parallel",
                "Branches": branches,
                "ResultPath": f"$.layer_{i}_result",
            }

    for i, name in enumerate(layer_names[:-1]):
        if "Next" not in states[name]:
            states[name]["Next"] = layer_names[i + 1]

    states["PipelineComplete"] = {"Type": "Succeed"}
    if layer_names:
        last = layer_names[-1]
        if states[last].get("Type") == "Parallel":
            states[last]["Next"] = "PipelineComplete"
        else:
            states[last]["Next"] = "PipelineComplete"
    else:
        layer_names.append("PipelineComplete")

    error_handler = {
        "Type": "Fail",
        "Cause": "A Glue job failed during the pipeline execution",
        "Error": "GlueJobFailure",
    }
    states["HandleError"] = error_handler

    for name, state in states.items():
        if state.get("Type") == "Task":
            state["Catch"] = [
                {
                    "ErrorEquals": ["States.ALL"],
                    "Next": "HandleError",
                    "ResultPath": f"$.error_{name}",
                }
            ]
        elif state.get("Type") == "Parallel":
            state["Catch"] = [
                {
                    "ErrorEquals": ["States.ALL"],
                    "Next": "HandleError",
                    "ResultPath": f"$.error_{name}",
                }
            ]

    definition = {
        "Comment": f"Pipeline: {config.pipeline_name}",
        "StartAt": layer_names[0] if layer_names else "PipelineComplete",
        "States": states,
    }

    return definition


def _build_steps(
    graph: DependencyGraph,
    config: PipelineConfig,
    *,
    lambda_scripts: set[str] | None = None,
) -> list[JobStep]:
    steps = []
    for node in graph.execution_order or graph.nodes:
        stem = Path(node).stem
        is_lambda = lambda_scripts and stem in lambda_scripts
        target = "lambda" if is_lambda else "glue"

        if is_lambda:
            job_name = f"{config.lambda_function_prefix}-{stem}"
        else:
            job_name = f"{config.glue_job_prefix}-{stem}"

        deps = [
            Path(e.source_file).stem
            for e in graph.edges
            if e.target_file == node
        ]

        steps.append(
            JobStep(
                name=stem,
                job_name=job_name,
                target=target,
                depends_on=deps,
            )
        )
    return steps


def _topological_layers(steps: list[JobStep]) -> list[list[JobStep]]:
    """Group steps into layers where each layer can run in parallel."""
    step_map = {s.name: s for s in steps}
    completed: set[str] = set()
    layers: list[list[GlueJobStep]] = []
    remaining = list(steps)

    while remaining:
        ready = [
            s for s in remaining if all(d in completed for d in s.depends_on)
        ]
        if not ready:
            ready = remaining[:1]
        layers.append(ready)
        for s in ready:
            completed.add(s.name)
        remaining = [s for s in remaining if s.name not in completed]

    return layers


def _glue_task_state(step: JobStep, config: PipelineConfig) -> dict:
    return {
        "Type": "Task",
        "Resource": "arn:aws:states:::glue:startJobRun.sync",
        "Parameters": {
            "JobName": step.job_name,
            "Arguments": {
                "--JOB_NAME": step.job_name,
                **{f"--{k}": v for k, v in step.arguments.items()},
            },
        },
        "TimeoutSeconds": config.job_timeout_minutes * 60,
        "Retry": [
            {
                "ErrorEquals": ["Glue.ConcurrentRunsExceededException"],
                "IntervalSeconds": config.retry_interval_seconds,
                "MaxAttempts": config.retry_attempts,
                "BackoffRate": 2.0,
            }
        ],
        "ResultPath": f"$.result_{step.name}",
    }


def _lambda_task_state(step: JobStep, config: PipelineConfig) -> dict:
    return {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {
            "FunctionName": step.job_name,
            "Payload.$": "$",
        },
        "TimeoutSeconds": config.lambda_timeout_seconds,
        "Retry": [
            {
                "ErrorEquals": ["Lambda.TooManyRequestsException"],
                "IntervalSeconds": config.retry_interval_seconds,
                "MaxAttempts": config.retry_attempts,
                "BackoffRate": 2.0,
            }
        ],
        "ResultPath": f"$.result_{step.name}",
    }


def save_pipeline(definition: dict, output_path: str | Path) -> None:
    """Save a Step Functions definition to a JSON file."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(definition, indent=2))


def generate_all_pipelines(
    graph: DependencyGraph,
    output_dir: str | Path,
    glue_job_prefix: str = "sas-migration",
) -> list[str]:
    """Generate pipeline definitions, splitting large graphs into sub-pipelines."""
    output_dir = Path(output_dir)
    generated: list[str] = []

    config = PipelineConfig(
        pipeline_name="sas-migration-main",
        glue_job_prefix=glue_job_prefix,
    )
    definition = generate_pipeline_from_graph(graph, config)
    path = output_dir / "main_pipeline.json"
    save_pipeline(definition, path)
    generated.append(str(path))

    return generated
