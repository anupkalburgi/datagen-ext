# Developer Guide

## Prerequisites
- Python 3.9.21 or higher
- [uv](https://github.com/astral-sh/uv) package manager

## Setup

1. Clone the repository:
```bash
git clone https://github.com/anupkalburgi/datagen-ext.git
cd datagen-ext
```

2. Create and activate a virtual environment using uv:
```bash
uv venv
source .venv/bin/activate
```

3. Install dependencies:
```bash
uv pip install -e ".[dev]"
```

## Development

### Running Tests
```bash
pytest
```

### Building the Package
```bash
uv pip install build
python -m build
```

### Installing in Development Mode
```bash
uv pip install -e .
```

## Project Structure
- `src/datagen_ext/`: Main package code
  - `generator.py`: Core data generation logic
  - `models.py`: Data models and schemas
  - `config_loader.py`: Configuration handling
  - `spec_generators/`: Custom data generators

## Dependencies
- Core: dbldatagen, pydantic
- Development: pyspark, pytest, jupyterlab, pandas

## License
MIT
