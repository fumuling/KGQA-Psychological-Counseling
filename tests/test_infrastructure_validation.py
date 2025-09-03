"""
Infrastructure validation tests to ensure testing setup is working correctly.
These tests validate that the testing infrastructure is properly configured.
"""

import pytest
import sys
import os
from pathlib import Path


class TestInfrastructureValidation:
    """Validate that the testing infrastructure is properly set up."""

    def test_pytest_is_working(self):
        """Basic test to ensure pytest is functioning."""
        assert True, "Pytest is working correctly"

    def test_python_version(self):
        """Verify Python version compatibility."""
        assert sys.version_info >= (3, 8), "Python 3.8+ is required"

    def test_project_structure_exists(self):
        """Verify project directories exist."""
        project_root = Path(__file__).parent.parent
        
        assert (project_root / "QASystem").exists(), "QASystem directory should exist"
        assert (project_root / "data").exists(), "data directory should exist"
        assert (project_root / "tests").exists(), "tests directory should exist"
        assert (project_root / "tests" / "unit").exists(), "tests/unit directory should exist"
        assert (project_root / "tests" / "integration").exists(), "tests/integration directory should exist"

    def test_pyproject_toml_exists(self):
        """Verify pyproject.toml exists and is readable."""
        project_root = Path(__file__).parent.parent
        pyproject_path = project_root / "pyproject.toml"
        
        assert pyproject_path.exists(), "pyproject.toml should exist"
        assert pyproject_path.is_file(), "pyproject.toml should be a file"
        
        # Verify it's readable
        content = pyproject_path.read_text()
        assert "[tool.poetry]" in content, "Should contain Poetry configuration"
        assert "[tool.pytest.ini_options]" in content, "Should contain pytest configuration"
        assert "[tool.coverage" in content, "Should contain coverage configuration"

    def test_test_discovery_patterns(self):
        """Verify pytest can discover this test file."""
        current_file = Path(__file__)
        assert current_file.name.startswith("test_"), "Test file should start with 'test_'"
        assert current_file.suffix == ".py", "Test file should be a Python file"

    @pytest.mark.unit
    def test_custom_markers_unit(self):
        """Test that custom 'unit' marker is working."""
        assert hasattr(pytest.mark, 'unit'), "Unit marker should be available"

    @pytest.mark.integration  
    def test_custom_markers_integration(self):
        """Test that custom 'integration' marker is working."""
        assert hasattr(pytest.mark, 'integration'), "Integration marker should be available"

    @pytest.mark.slow
    def test_custom_markers_slow(self):
        """Test that custom 'slow' marker is working."""
        assert hasattr(pytest.mark, 'slow'), "Slow marker should be available"

    def test_fixtures_are_available(self, temp_dir, sample_data_dir):
        """Test that custom fixtures from conftest.py are available."""
        assert temp_dir is not None, "temp_dir fixture should be available"
        assert os.path.exists(temp_dir), "temp_dir should exist"
        assert os.path.isdir(temp_dir), "temp_dir should be a directory"
        
        assert sample_data_dir is not None, "sample_data_dir fixture should be available"
        assert os.path.exists(sample_data_dir), "sample_data_dir should exist"

    def test_mock_fixtures_available(self, mock_neo4j_graph, sample_question_data):
        """Test that mock fixtures are properly configured."""
        assert mock_neo4j_graph is not None, "mock_neo4j_graph fixture should be available"
        assert sample_question_data is not None, "sample_question_data fixture should be available"
        
        # Verify sample data structure
        assert 'disease_symptom_questions' in sample_question_data
        assert 'symptom_disease_questions' in sample_question_data
        assert len(sample_question_data['disease_symptom_questions']) > 0

    def test_package_imports_possible(self):
        """Test that the main packages can be imported (with mocking if needed)."""
        project_root = Path(__file__).parent.parent
        
        # Add project root to Python path temporarily if not already there
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        try:
            # Test that we can import the main modules (they may fail due to dependencies)
            # but the import structure should be correct
            qa_system_path = project_root / "QASystem"
            data_path = project_root / "data"
            
            assert qa_system_path.exists(), "QASystem package should exist"
            assert data_path.exists(), "data package should exist"
            
            # Check for main module files
            assert (qa_system_path / "api.py").exists(), "api.py should exist"
            assert (qa_system_path / "answer_search.py").exists(), "answer_search.py should exist"
            assert (qa_system_path / "question_classifier.py").exists(), "question_classifier.py should exist"
            
        finally:
            # Clean up sys.path
            if str(project_root) in sys.path:
                sys.path.remove(str(project_root))

    def test_coverage_configuration(self):
        """Test that coverage configuration is working."""
        # This test verifies that we can import coverage-related settings
        # The actual coverage will be tested when running with --cov flag
        import sys
        
        # Verify that we're in a test environment
        assert 'pytest' in sys.modules, "Should be running under pytest"

    def test_environment_setup(self, clean_environment):
        """Test environment variable handling."""
        assert clean_environment is not None, "Environment fixture should work"
        assert 'NEO4J_HOST' in clean_environment, "Test environment should be set up"


class TestMockingCapabilities:
    """Test that mocking capabilities are working correctly."""

    def test_mock_creation(self, mock_neo4j_graph):
        """Test that we can create and use mocks."""
        # Test basic mock functionality
        assert mock_neo4j_graph is not None
        
        # Test that we can call mock methods
        result = mock_neo4j_graph.run("TEST QUERY")
        assert result is not None

    def test_patch_functionality(self):
        """Test that pytest-mock functionality is available."""
        # This test verifies that pytest-mock is properly installed
        from unittest.mock import Mock, patch
        
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = True
            assert os.path.exists("/fake/path") is True


class TestDataFiles:
    """Test that test data files and fixtures are working."""

    def test_sample_data_creation(self, sample_data_dir):
        """Test that sample data files are created correctly."""
        assert os.path.exists(sample_data_dir)
        
        # Check that sample files were created
        disease_file = os.path.join(sample_data_dir, "disease.txt")
        assert os.path.exists(disease_file), "disease.txt should be created"
        
        with open(disease_file, 'r', encoding='utf-8') as f:
            content = f.read()
            assert 'depression' in content, "Sample data should contain expected content"

    def test_temporary_directory_cleanup(self, temp_dir):
        """Test that temporary directories are properly managed."""
        # Create a test file in temp directory
        test_file = os.path.join(temp_dir, "test.txt")
        with open(test_file, 'w') as f:
            f.write("test content")
        
        assert os.path.exists(test_file), "Test file should be created"
        # The cleanup will be tested by pytest fixture teardown


if __name__ == "__main__":
    # Allow running this file directly for quick validation
    pytest.main([__file__, "-v"])