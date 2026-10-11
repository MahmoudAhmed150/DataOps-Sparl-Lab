# DataOps Spark Lab

An end-to-end DataOps project demonstrating a PySpark data pipeline, automated testing, version control, containerization, and CI/CD automation using GitHub Actions.

## Project Overview

This project processes customer and order data using PySpark. It applies data cleaning and filtering rules, calculates order revenue, joins datasets, and aggregates revenue by country.

The project demonstrates how to build, test, package, and continuously validate a data processing pipeline.

## Tech Stack

- **Python** — Pipeline implementation
- **Apache Spark (PySpark)** — Distributed data processing
- **Pytest** — Automated testing
- **Git & GitHub** — Version control and collaboration
- **Docker** — Containerization
- **GitHub Actions** — Continuous Integration and Continuous Delivery
- **Docker Hub** — Container image registry

## Data Pipeline

The pipeline performs the following steps:

1. Reads customer and order CSV files.
2. Cleans customer records by filtering out customers under 18.
3. Filters orders to include completed orders with a quantity of at least 2.
4. Calculates revenue using quantity multiplied by price.
5. Joins order records with customer data.
6. Aggregates revenue by country and sorts the results in descending order.

## Testing

Pytest is used to validate the pipeline's transformations and data quality rules.

The test suite covers:

- Customer data cleaning
- Order data filtering
- Revenue calculation
- Revenue aggregation
- Schema validation
- Missing-column validation

Run the complete test suite locally:

```bash
python -m pytest -v
```

Run a specific test:

```bash
python -m pytest tests/test_spark_job.py::test_clean_customers -v
```

## Docker

The application is containerized using Docker.

Build the image:

```bash
docker build -t dataops-spark-lab .
```

Run the container:

```bash
docker run --rm dataops-spark-lab
```

## CI/CD with GitHub Actions

The project uses GitHub Actions to automate testing and container image publishing.

### Continuous Integration (CI)

The CI workflow:

1. Checks out the repository.
2. Sets up Python.
3. Installs project dependencies.
4. Runs the automated test suite.

The workflow runs on pushes to applicable branches and pull requests targeting the configured integration branches.

### Continuous Delivery (CD)

The CD workflow runs when changes are pushed to `main`.

It:

1. Runs the automated tests.
2. Builds the Docker image.
3. Publishes the image to Docker Hub only if the test job succeeds.

The Docker publishing credentials are stored as GitHub Actions secrets.

## Docker Hub

The published image is available at:

[mahmoud1511/dataops-spark-lab](https://hub.docker.com/r/mahmoud1511/dataops-spark-lab)

Pull the latest image:

```bash
docker pull mahmoud1511/dataops-spark-lab:latest
```

Run it:

```bash
docker run --rm mahmoud1511/dataops-spark-lab:latest
```

## Repository Structure

```text
DataOps-Sparl-Lab/
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── cd.yml
├── data/
│   ├── customers.csv
│   └── orders.csv
├── src/
│   └── spark_job.py
├── tests/
│   └── test_spark_job.py
├── Dockerfile
├── requirements.txt
└── README.md
```

## Learning Objectives

- Build a data processing pipeline with PySpark.
- Write automated tests for data transformations.
- Use Git branches and pull requests for controlled integration.
- Containerize a Python data application.
- Automate testing and image publishing with GitHub Actions.
- Practice failure detection and recovery through CI feedback.

## Author

**Mahmoud Ahmed**

GitHub: [MahmoudAhmed150](https://github.com/MahmoudAhmed150)