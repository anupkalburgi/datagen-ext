# datagen-ext

A new Python project setup using uv and pyproject.toml.

## Prerequisites

*   [uv](https://github.com/astral-sh/uv): Ensure `uv` is installed.
*   Python 3.8+

## Setup & Installation

1.  **Clone the Repository (if not already done):**
    ```bash
    git clone https://github.com/anupkalburgi/datagen-ext.git # Update URL
    cd datagen-ext
    ```

2.  **Create/Activate Virtual Environment:**
    This script already created a `venv` directory. Activate it:
    ```bash
    source venv/bin/activate
    ```
    *(If setting up manually later, create one with `uv venv venv`)*

3.  **Install Project Dependencies:**
    Install the project in editable mode along with development dependencies using `uv`:
    ```bash
    uv pip install -e .[dev]
    ```
    *   `-e`: Installs in editable mode (changes in `src/` are reflected immediately).
    *   `.`: Refers to the current directory (where `pyproject.toml` is).
    *   `[dev]`: Installs the optional dependencies listed under `[project.optional-dependencies.dev]` in `pyproject.toml`.

4.  **Adding Dependencies:**
    *   To add a **runtime** dependency (needed for the project to run), add it to the `dependencies = [...] ` list in `pyproject.toml` and re-run `uv pip install -e .[dev]`.
    *   To add a **development** dependency (like linters, formatters), add it to the `dev = [...] ` list in `[project.optional-dependencies]` and re-run `uv pip install -e .[dev]`.

## Running the Example CLI

After installation, the example script defined in `pyproject.toml` should be available.
Make sure your virtual environment is activated.
```bash
# Run the script defined under [project.scripts]
datagen-ext 42
datagen-ext hello
```

## Running Tests

Tests are written using `pytest`.

1.  **Activate Environment:** Ensure your virtual environment (`venv`) is activated.
2.  **Run Tests:** Execute `pytest` from the project root directory:
    ```bash
    pytest
    ```
    *To run with more verbose output:* `pytest -v`

## IDE Configuration (VS Code / PyCharm)

Use the Python interpreter path found in `.env_info.txt` (`datagen-extended/datagen-ext/venv/bin/python`) to configure your IDE for this project.


### Sample Run
```
datagen-ext -c /Users/anup.kalburgi/code/datagen-extended/datagen-ext/configs/simple_table.json
2025-04-04 15:55:32 - datagen_ext.__main__ - INFO - Starting data generation process with config: /Users/anup.kalburgi/code/datagen-extended/datagen-ext/configs/simple_table.json
2025-04-04 15:55:32 - datagen_ext.__main__ - INFO - Configuration loaded and validated successfully from '/Users/anup.kalburgi/code/datagen-extended/datagen-ext/configs/simple_table.json'
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
25/04/04 15:55:34 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
2025-04-04 15:55:35 - datagen_ext.generator - INFO - SparkSession initialized successfully.
2025-04-04 15:55:35 - datagen_ext.generator - INFO - Processing tables in order: ['Users']
2025-04-04 15:55:35 - datagen_ext.generator - INFO - --- Starting generation for table: Users ---
2025-04-04 15:55:35 - datagen_ext.generator - INFO - Table 'Users': Building DataFrame...
2025-04-04 15:55:37 - datagen_ext.generator - INFO - Table 'Users': Successfully built DataFrame with 1000 rows.
2025-04-04 15:55:37 - datagen_ext.generator - INFO - Table 'Users': Writing parquet data to: /Users/anup.kalburgi/code/datagen-extended/datagen-ext/synthetic_data_deterministic/Users
2025-04-04 15:55:39 - datagen_ext.generator - INFO - Table 'Users': Finished writing.
2025-04-04 15:55:40 - datagen_ext.generator - INFO - SparkSession stopped.
2025-04-04 15:55:40 - datagen_ext.generator - INFO - --- Data generation completed successfully. ---
(datagen-ext) ➜  datagen-ext git:(master) ✗

```

## Publishing to PyPI (Future Steps)

1.  Update `pyproject.toml` with accurate metadata (author, license, description, URLs, classifiers).
2.  Choose and add a `LICENSE` file.
3.  Ensure your runtime `dependencies` are correct.
4.  Install build tools: `uv pip install build twine` (if not already in dev dependencies).
5.  Build the package: `python -m build`
6.  Upload to TestPyPI first, then PyPI using `twine upload dist/*`.

