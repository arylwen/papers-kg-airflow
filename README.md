# Airflow DAGs for Academic Paper Processing

![Airflow Logo](docs/papers-kg-logo.png)

Welcome to the **Airflow DAGs for Academic Paper Processing** repository! In this project, we've created a set of [Apache Airflow](https://airflow.apache.org/) Directed Acyclic Graphs (DAGs) that automate various tasks related to academic papers sourced from Mastodon bots. Whether you're a researcher, developer, or just curious about the intersection of automation and scholarly content, this repository has something for you.

## Key Features

1. **Mastodon Bot Monitoring**: Our DAGs continuously monitor Mastodon bots for new posts related to academic papers. Whenever a new post is detected, the DAGs swing into action.

2. **PDF Download and Conversion**: We automatically download PDF files linked in Mastodon posts. These files are then converted into plain text for further processing.

3. **Text Cleanup and Preprocessing**: The extracted text undergoes cleaning and preprocessing. We remove noise, fix formatting issues, and ensure the content is ready for analysis.

4. **AI Knowledge Graph Indexing**: Leveraging state-of-the-art natural language processing (NLP) techniques, we build an AI knowledge graph. This graph connects academic concepts, authors, and papers, providing a rich semantic network.

5. **Amazon S3 Integration**: The final processed data is uploaded to Amazon S3, making it accessible for downstream applications, analytics, or visualization.

## Getting Started

1. **Prerequisites**:
    - Ensure you have [Python](https://www.python.org/downloads/) and [Docker](https://www.docker.com/) installed.
    - Familiarize yourself with [Apache Airflow](https://airflow.apache.org/).

2. **Installation**:
    - Clone this repository to your local machine.
    - Set up your Airflow environment using Docker Compose:
        ```bash
        docker-compose up -d
        ```

3. **Configuration**:
    - Customize the DAGs by editing the Python files in the `dags` directory.
    - Each DAG has a unique ID, making it easy to manage and schedule.

4. **Running the DAGs**:
    - Start the Airflow web server:
        ```bash
        docker-compose exec webserver airflow webserver
        ```
    - Access the Airflow UI at [http://localhost:8080](http://localhost:8080).
    - Enable and trigger your desired DAGs.

## Contributing

We welcome contributions! Whether you want to add new features, improve existing DAGs, or fix bugs, feel free to submit pull requests. Let's build a robust and efficient system together!

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Happy automating!** 🚀📚🤖