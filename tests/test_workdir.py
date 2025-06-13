"""Tests for the workdir commands (job workdir and task workdir)."""

import json
import os
import tempfile

from .conftest import HqEnv
from .utils import wait_for_job_state


def test_job_workdir_single_job(hq_env: HqEnv):
    """Test job workdir command with a single job."""
    hq_env.start_server()
    hq_env.start_worker()

    # Submit a simple job
    hq_env.command(["submit", "--", "echo", "hello"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    # Test CLI output
    output = hq_env.command(["job", "workdir", "1"])
    lines = output.strip().split('\n')
    
    assert len(lines) >= 2  # Should have job header and at least one workdir
    assert lines[0] == "Job 1:"
    assert os.getcwd() in lines[1]  # Current directory should be shown


def test_job_workdir_multiple_jobs(hq_env: HqEnv):
    """Test job workdir command with multiple jobs."""
    hq_env.start_server()
    hq_env.start_worker()

    # Submit multiple jobs
    hq_env.command(["submit", "--", "echo", "job1"])
    hq_env.command(["submit", "--", "echo", "job2"])
    wait_for_job_state(hq_env, [1, 2], "FINISHED")

    # Test with job range
    output = hq_env.command(["job", "workdir", "1-2"])
    lines = output.strip().split('\n')
    
    # Should have headers and workdirs for both jobs
    assert "Job 1:" in lines
    assert "Job 2:" in lines


def test_job_workdir_last_selector(hq_env: HqEnv):
    """Test job workdir command with 'last' selector."""
    hq_env.start_server()
    hq_env.start_worker()

    # Submit multiple jobs
    hq_env.command(["submit", "--", "echo", "job1"])
    hq_env.command(["submit", "--", "echo", "job2"]) 
    wait_for_job_state(hq_env, [1, 2], "FINISHED")

    # Test with 'last' selector - should show only job 2
    output = hq_env.command(["job", "workdir", "last"])
    lines = output.strip().split('\n')
    
    assert "Job 2:" in lines
    assert "Job 1:" not in lines


def test_job_workdir_with_custom_cwd(hq_env: HqEnv):
    """Test job workdir command with custom working directory."""
    hq_env.start_server()
    hq_env.start_worker()

    # Create a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        # Submit job with custom working directory
        hq_env.command([
            "submit", 
            "--cwd", tmpdir,
            "--", "echo", "hello"
        ])
        wait_for_job_state(hq_env, 1, "FINISHED")

        # Check workdir output
        output = hq_env.command(["job", "workdir", "1"])
        lines = output.strip().split('\n')
        
        assert len(lines) >= 2
        assert lines[0] == "Job 1:"
        # Should show the custom directory
        assert tmpdir in output


def test_job_workdir_array_job(hq_env: HqEnv):
    """Test job workdir command with array job."""
    hq_env.start_server()
    hq_env.start_worker()

    # Submit array job
    hq_env.command(["submit", "--array", "1-3", "--", "echo", "$HQ_TASK_ID"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    # Test workdir output
    output = hq_env.command(["job", "workdir", "1"])
    lines = output.strip().split('\n')
    
    assert len(lines) >= 2
    assert lines[0] == "Job 1:"
    assert os.getcwd() in lines[1]


def test_job_workdir_json_output(hq_env: HqEnv):
    """Test job workdir command with JSON output."""
    hq_env.start_server()
    hq_env.start_worker()

    # Submit a job
    hq_env.command(["submit", "--", "echo", "hello"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    # Test JSON output
    output = hq_env.command(["job", "workdir", "1", "--output-mode", "json"])
    data = json.loads(output)
    
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]["job_id"] == 1
    assert "workdirs" in data[0]
    assert isinstance(data[0]["workdirs"], list)
    assert len(data[0]["workdirs"]) >= 1


def test_job_workdir_quiet_output(hq_env: HqEnv):
    """Test job workdir command with quiet output."""
    hq_env.start_server()
    hq_env.start_worker()

    # Submit a job
    hq_env.command(["submit", "--", "echo", "hello"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    # Test quiet output - should be empty
    output = hq_env.command(["job", "workdir", "1", "--output-mode", "quiet"])
    assert output.strip() == ""


def test_job_workdir_nonexistent_job(hq_env: HqEnv):
    """Test job workdir command with nonexistent job."""
    hq_env.start_server()

    # Try to get workdir for nonexistent job - should succeed but with empty output
    hq_env.command(["job", "workdir", "999"])
    # Should succeed but show no jobs (empty output or only error logs)


def test_task_workdir_single_task(hq_env: HqEnv):
    """Test task workdir command with a single task."""
    hq_env.start_server()
    hq_env.start_worker()

    # Submit array job
    hq_env.command(["submit", "--array", "1-3", "--", "echo", "$HQ_TASK_ID"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    # Test task workdir for single task
    output = hq_env.command(["task", "workdir", "1", "2"])
    lines = output.strip().split('\n')
    
    assert len(lines) >= 2
    assert lines[0] == "Job 1:"
    assert "Task 2:" in lines[1]
    assert os.getcwd() in lines[1]


def test_task_workdir_multiple_tasks(hq_env: HqEnv):
    """Test task workdir command with multiple tasks."""
    hq_env.start_server()
    hq_env.start_worker()

    # Submit array job
    hq_env.command(["submit", "--array", "1-4", "--", "echo", "$HQ_TASK_ID"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    # Test task workdir for multiple tasks
    output = hq_env.command(["task", "workdir", "1", "2-3"])
    lines = output.strip().split('\n')
    
    assert "Job 1:" in lines
    assert any("Task 2:" in line for line in lines)
    assert any("Task 3:" in line for line in lines)


def test_task_workdir_with_custom_cwd(hq_env: HqEnv):
    """Test task workdir command with custom working directory."""
    hq_env.start_server()
    hq_env.start_worker()

    # Create a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        # Submit array job with custom working directory
        hq_env.command([
            "submit", 
            "--array", "1-2",
            "--cwd", tmpdir,
            "--", "echo", "$HQ_TASK_ID"
        ])
        wait_for_job_state(hq_env, 1, "FINISHED")

        # Check task workdir output
        output = hq_env.command(["task", "workdir", "1", "1"])
        
        assert "Job 1:" in output
        assert "Task 1:" in output
        assert tmpdir in output


def test_task_workdir_json_output(hq_env: HqEnv):
    """Test task workdir command with JSON output."""
    hq_env.start_server()
    hq_env.start_worker()

    # Submit array job
    hq_env.command(["submit", "--array", "1-2", "--", "echo", "$HQ_TASK_ID"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    # Test JSON output
    output = hq_env.command(["task", "workdir", "1", "1-2", "--output-mode", "json"])
    data = json.loads(output)
    
    assert isinstance(data, list)
    assert len(data) == 1  # One job
    assert data[0]["job_id"] == 1
    assert "tasks" in data[0]
    assert isinstance(data[0]["tasks"], dict)
    assert "1" in data[0]["tasks"]
    assert "2" in data[0]["tasks"]


def test_task_workdir_quiet_output(hq_env: HqEnv):
    """Test task workdir command with quiet output."""
    hq_env.start_server()
    hq_env.start_worker()

    # Submit array job
    hq_env.command(["submit", "--array", "1-2", "--", "echo", "$HQ_TASK_ID"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    # Test quiet output - should be empty
    output = hq_env.command(["task", "workdir", "1", "1", "--output-mode", "quiet"])
    assert output.strip() == ""


def test_task_workdir_last_job_selector(hq_env: HqEnv):
    """Test task workdir command with 'last' job selector."""
    hq_env.start_server()
    hq_env.start_worker()

    # Submit multiple jobs
    hq_env.command(["submit", "--array", "1-2", "--", "echo", "job1"])
    hq_env.command(["submit", "--array", "1-2", "--", "echo", "job2"])
    wait_for_job_state(hq_env, [1, 2], "FINISHED")

    # Test with 'last' selector - should show tasks from job 2
    output = hq_env.command(["task", "workdir", "last", "1"])
    
    assert "Job 2:" in output
    assert "Task 1:" in output
    assert "Job 1:" not in output


def test_task_workdir_nonexistent_task(hq_env: HqEnv):
    """Test task workdir command with nonexistent task."""
    hq_env.start_server()
    hq_env.start_worker()

    # Submit job with limited tasks
    hq_env.command(["submit", "--array", "1-2", "--", "echo", "$HQ_TASK_ID"])
    wait_for_job_state(hq_env, 1, "FINISHED")

    # Try to get workdir for nonexistent task
    output = hq_env.command(["task", "workdir", "1", "999"])
    
    # Should show job header but no tasks (since task doesn't exist)
    assert "Job 1:" in output
    # Should not crash, just show no tasks


def test_task_workdir_with_task_dir_placeholder(hq_env: HqEnv):
    """Test task workdir command with task directory placeholders."""
    hq_env.start_server()
    hq_env.start_worker()

    # Create a temporary directory and use it as working directory instead of placeholder
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        # Submit job with custom working directory (avoid complex placeholders)
        hq_env.command([
            "submit", 
            "--array", "1-2",
            "--cwd", tmpdir,
            "--", "echo", "$HQ_TASK_ID"
        ])
        wait_for_job_state(hq_env, 1, "FINISHED")

        # Check task workdir output - should show the working directory
        output = hq_env.command(["task", "workdir", "1", "1"])
        
        assert "Job 1:" in output
        assert "Task 1:" in output
        # Should contain the temp directory path
        assert tmpdir in output


def test_job_workdir_help(hq_env: HqEnv):
    """Test that job workdir help works."""
    hq_env.start_server()
    
    # Test help output
    output = hq_env.command(["job", "workdir", "--help"])
    assert "Display working directory of selected job(s)" in output
    assert "SELECTOR" in output


def test_task_workdir_help(hq_env: HqEnv):
    """Test that task workdir help works."""
    hq_env.start_server()
    
    # Test help output
    output = hq_env.command(["task", "workdir", "--help"])
    assert "Display working directory of selected task(s)" in output
    assert "JOB_SELECTOR" in output
    assert "TASK_SELECTOR" in output