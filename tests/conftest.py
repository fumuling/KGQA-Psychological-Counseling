"""
Shared pytest fixtures for the KGQA Psychological Counseling system tests.
"""

import pytest
import tempfile
import os
import sys
from unittest.mock import Mock, patch
from pathlib import Path

# Add the project root to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def temp_dir():
    """Provide a temporary directory that is cleaned up after test."""
    with tempfile.TemporaryDirectory() as temp_path:
        yield temp_path


@pytest.fixture
def sample_data_dir(temp_dir):
    """Create a temporary directory with sample data files."""
    data_dir = os.path.join(temp_dir, "dict")
    os.makedirs(data_dir, exist_ok=True)
    
    # Create sample dictionary files
    sample_files = {
        "disease.txt": ["depression", "anxiety", "bipolar", "ptsd"],
        "symptom.txt": ["sadness", "fear", "mood swings", "nightmares"],
        "department.txt": ["psychiatry", "psychology", "neurology"],
        "check.txt": ["mental health screening", "psychological assessment"],
        "deny.txt": ["not", "never", "no"]
    }
    
    for filename, content in sample_files.items():
        file_path = os.path.join(data_dir, filename)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(content))
    
    return data_dir


@pytest.fixture
def mock_neo4j_graph():
    """Mock py2neo Graph connection."""
    with patch('py2neo.Graph') as mock_graph:
        mock_instance = Mock()
        mock_graph.return_value = mock_instance
        
        # Configure mock responses for common queries
        mock_instance.run.return_value.data.return_value = [
            {'m.name': 'depression', 'n.name': 'sadness'},
            {'m.name': 'depression', 'n.name': 'fatigue'}
        ]
        
        yield mock_instance


@pytest.fixture
def mock_flask_app():
    """Mock Flask application for testing."""
    with patch('flask.Flask') as mock_flask:
        mock_app = Mock()
        mock_flask.return_value = mock_app
        mock_app.config = {}
        yield mock_app


@pytest.fixture
def sample_question_data():
    """Sample question data for testing question processing."""
    return {
        'disease_symptom_questions': [
            "抑郁症有什么症状？",
            "焦虑症的表现是什么？"
        ],
        'symptom_disease_questions': [
            "我总是感到悲伤，这可能是什么病？",
            "恐惧感可能是什么疾病的症状？"
        ],
        'disease_prevent_questions': [
            "如何预防抑郁症？",
            "焦虑症的预防措施有哪些？"
        ]
    }


@pytest.fixture
def mock_ahocorasick():
    """Mock ahocorasick library for string matching."""
    with patch('ahocorasick.Automaton') as mock_ac:
        mock_instance = Mock()
        mock_ac.return_value = mock_instance
        
        # Mock the iter method to return sample matches
        mock_instance.iter.return_value = [
            (8, 'depression'),  # (end_index, pattern)
            (15, 'anxiety')
        ]
        
        yield mock_instance


@pytest.fixture
def sample_classification_result():
    """Sample classification result for testing."""
    return {
        'question_types': ['disease_symptom'],
        'entities': ['depression']
    }


@pytest.fixture
def sample_sql_result():
    """Sample SQL generation result for testing."""
    return [{
        'question_type': 'disease_symptom',
        'sql': ['MATCH (m:Disease)-[:HAS_SYMPTOM]->(n:Symptom) WHERE m.name = "depression" RETURN m.name, n.name']
    }]


@pytest.fixture
def sample_neo4j_response():
    """Sample Neo4j database response."""
    return [
        {'m.name': 'depression', 'n.name': 'persistent sadness'},
        {'m.name': 'depression', 'n.name': 'loss of interest'},
        {'m.name': 'depression', 'n.name': 'fatigue'},
        {'m.name': 'depression', 'n.name': 'sleep disturbances'}
    ]


@pytest.fixture
def expected_answer():
    """Expected formatted answer for testing."""
    return "抑郁症的症状包括：persistent sadness；loss of interest；fatigue；sleep disturbances"


@pytest.fixture(autouse=True)
def reset_imports():
    """Reset imports to ensure clean test state."""
    # Store original modules
    original_modules = sys.modules.copy()
    
    yield
    
    # Clean up any test-specific modules
    modules_to_remove = [
        mod for mod in sys.modules.keys() 
        if mod.startswith('QASystem') or mod.startswith('data')
    ]
    
    for mod in modules_to_remove:
        if mod in original_modules:
            sys.modules[mod] = original_modules[mod]
        else:
            del sys.modules[mod]


@pytest.fixture
def mock_file_operations():
    """Mock file operations for testing."""
    with patch('builtins.open', create=True) as mock_open, \
         patch('os.path.exists') as mock_exists, \
         patch('os.listdir') as mock_listdir:
        
        mock_exists.return_value = True
        mock_listdir.return_value = ['disease.txt', 'symptom.txt']
        mock_open.return_value.__enter__.return_value.read.return_value = "test\ncontent"
        
        yield {
            'open': mock_open,
            'exists': mock_exists,
            'listdir': mock_listdir
        }


@pytest.fixture
def clean_environment():
    """Ensure clean environment variables for testing."""
    original_env = os.environ.copy()
    
    # Set test-specific environment variables
    test_env = {
        'NEO4J_HOST': 'localhost',
        'NEO4J_PORT': '7474',
        'NEO4J_USER': 'test_user',
        'NEO4J_PASSWORD': 'test_password'
    }
    
    os.environ.update(test_env)
    
    yield test_env
    
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)