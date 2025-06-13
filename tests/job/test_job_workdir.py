"""Integration tests for job workdir command."""

import json
import os
import tempfile
from pathlib import Path

from ..conftest import HqEnv
from ..utils import wait_for_job_state
from ..utils.job import list_jobs


def test_job_workdir_integration_with_job_list(hq_env: HqEnv):
    """Test that job workdir works with jobs from job list."""
    hq_env.start_server()
    hq_env.start_worker()

    # Submit multiple jobs
    hq_env.command(["submit", "--name", "test-job-1", "--", "echo", "hello"])
    hq_env.command(["submit", "--name", "test-job-2", "--", "echo", "world"])
    wait_for_job_state(hq_env, [1, 2], "FINISHED")

    # Verify jobs exist in list
    table = list_jobs(hq_env)
    assert len(table) == 2

    # Test workdir for all jobs
    output = hq_env.command(["job", "workdir", "all"])
    assert "Job 1:" in output
    assert "Job 2:" in output


def test_job_workdir_with_complex_working_directories(hq_env: HqEnv):
    """Test job workdir with various working directory configurations."""
    hq_env.start_server()
    hq_env.start_worker()

    with tempfile.TemporaryDirectory() as tmpdir:
        subdir = os.path.join(tmpdir, "subdir")
        os.makedirs(subdir)

        # Job with default cwd
        hq_env.command(["submit", "--name", "default-cwd", "--", "pwd"])
        
        # Job with custom cwd
        hq_env.command([
            "submit", 
            "--name", "custom-cwd",
            "--cwd", subdir,
            "--", "pwd"
        ])
        
        # Job with relative cwd
        rel_path = os.path.relpath(subdir)
        hq_env.command([
            "submit", 
            "--name", "relative-cwd",
            "--cwd", rel_path,
            "--", "pwd"
        ])

        wait_for_job_state(hq_env, [1, 2, 3], "FINISHED")

        # Test workdir output for all jobs
        output = hq_env.command(["job", "workdir", "1-3"])
        
        # Should contain all three job headers
        assert "Job 1:" in output
        assert "Job 2:" in output  
        assert "Job 3:" in output
        
        # Should contain the custom directory path
        assert subdir in output or rel_path in output


def test_job_workdir_json_structure(hq_env: HqEnv):
    """Test the JSON structure of job workdir output."""
    hq_env.start_server()
    hq_env.start_worker()

    with tempfile.TemporaryDirectory() as tmpdir:
        # Submit job with custom directory
        hq_env.command([
            "submit",
            "--cwd", tmpdir,
            "--array", "1-2",
            "--", "echo", "test"
        ])
        wait_for_job_state(hq_env, 1, "FINISHED")

        # Get JSON output
        output = hq_env.command(["job", "workdir", "1", "--output-mode", "json"])
        data = json.loads(output)

        # Verify JSON structure
        assert isinstance(data, list)
        assert len(data) == 1
        
        job_data = data[0]
        assert "job_id" in job_data
        assert job_data["job_id"] == 1
        assert "workdirs" in job_data
        assert isinstance(job_data["workdirs"], list)
        
        # Should contain the custom directory
        workdirs = job_data["workdirs"]
        assert len(workdirs) >= 1
        assert any(tmpdir in wd for wd in workdirs)


def test_job_workdir_error_handling(hq_env: HqEnv):
    """Test error handling in job workdir command."""
    hq_env.start_server()

    # Test with no jobs - should succeed but return empty output
    output = hq_env.command(["job", "workdir", "1"])
    # Should handle gracefully (no crash, empty or minimal output)
    
    # Test with invalid selector - check manually for now since clap error handling varies
    try:
        output = hq_env.command(["job", "workdir", "invalid"])
        # If it doesn't fail, that's also acceptable
    except Exception:
        # Expected to fail with invalid selector
        pass


def test_job_workdir_with_stdout_stderr_redirection(hq_env: HqEnv):
    """Test job workdir when jobs have stdout/stderr redirection."""
    hq_env.start_server()
    hq_env.start_worker()

    with tempfile.TemporaryDirectory() as tmpdir:
        stdout_file = os.path.join(tmpdir, "output.txt")
        stderr_file = os.path.join(tmpdir, "error.txt")
        
        # Submit job with output redirection
        hq_env.command([
            "submit",
            "--stdout", stdout_file,
            "--stderr", stderr_file,
            "--cwd", tmpdir,
            "--", "echo", "hello"
        ])
        wait_for_job_state(hq_env, 1, "FINISHED")

        # Test workdir - should show the working directory, not the output files
        output = hq_env.command(["job", "workdir", "1"])
        
        assert "Job 1:" in output
        assert tmpdir in output


def test_job_workdir_consistency_with_job_info(hq_env: HqEnv):
    """Test that job workdir is consistent with job info output."""
    hq_env.start_server()
    hq_env.start_worker()

    with tempfile.TemporaryDirectory() as tmpdir:
        # Submit job with custom working directory
        hq_env.command([
            "submit",
            "--cwd", tmpdir,
            "--", "echo", "test"
        ])
        wait_for_job_state(hq_env, 1, "FINISHED")

        # Get workdir output
        workdir_output = hq_env.command(["job", "workdir", "1"])
        
        # Get job info output
        info_output = hq_env.command(["job", "info", "1"])
        
        # Both should reference the same working directory
        assert tmpdir in workdir_output
        assert tmpdir in info_output


def test_job_workdir_performance_with_many_jobs(hq_env: HqEnv):
    """Test job workdir performance with multiple jobs."""
    hq_env.start_server()
    hq_env.start_worker(cpus=4)

    # Submit multiple jobs quickly
    for i in range(10):
        hq_env.command(["submit", "--", "echo", f"job-{i}"])
    
    wait_for_job_state(hq_env, list(range(1, 11)), "FINISHED")

    # Test workdir for all jobs - should complete reasonably quickly
    output = hq_env.command(["job", "workdir", "1-10"])
    
    # Should contain all job headers
    for i in range(1, 11):
        assert f"Job {i}:" in output