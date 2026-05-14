# Installation & Getting Started

To ensure a smooth and isolated environment for your data projects, we highly recommend using a Python Virtual Environment (`venv`).

## 1. Create a Virtual Environment (Optional but Recommended)

First, navigate to your project folder and create a fresh environment:

```bash
# Create the environment
python -m venv venv

# Activate it (Linux/macOS)
source venv/bin/activate

# Activate it (Windows)
.\venv\Scripts\activate

```

## 2. Install `pysqdb`

Install the latest version of `pysqdb` directly from PyPI. This will automatically install all necessary dependencies, including **DuckDB** and **Pandas**.

```bash
pip install pysqdb

```

> **Note:** `pysqdb` requires **Python 3.9+**. It is built on top of DuckDB, so you don't need to install DuckDB separately; we handle that for you!

## 3. Verify Installation

To make sure everything is set up correctly, run this quick check in your terminal or a Python script:

```python
import pysqdb as ps

# If this runs without error, you're ready to fly!
print(f"Pysqdb is ready! Connection test: {ps.connect(':memory:')}")

```
If you see a successful connection message, congratulations! You're all set to start using `pysqdb` for your data analysis and manipulation tasks.



