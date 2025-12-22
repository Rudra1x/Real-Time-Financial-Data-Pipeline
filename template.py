import os
from pathlib import Path

PROJECT_NAME = "src"

FILES = [

    f"{PROJECT_NAME}/__init__.py",


    f"{PROJECT_NAME}/configuration/__init__.py",
    f"{PROJECT_NAME}/configuration/kafka_config.py",
    f"{PROJECT_NAME}/configuration/spark_config.py",
    f"{PROJECT_NAME}/configuration/aws_config.py",
    f"{PROJECT_NAME}/configuration/redis_config.py",
    f"{PROJECT_NAME}/configuration/feast_config.py",


    f"{PROJECT_NAME}/logger/__init__.py",
    f"{PROJECT_NAME}/logger/logger.py",
    f"{PROJECT_NAME}/exception/__init__.py",
    f"{PROJECT_NAME}/exception/exception.py",


    f"{PROJECT_NAME}/constants/__init__.py",
    f"{PROJECT_NAME}/constants/constants.py",


    f"{PROJECT_NAME}/utils/__init__.py",
    f"{PROJECT_NAME}/utils/common.py",
    f"{PROJECT_NAME}/utils/schema_utils.py",
    f"{PROJECT_NAME}/utils/time_utils.py",


    f"{PROJECT_NAME}/entity/__init__.py",
    f"{PROJECT_NAME}/entity/config_entity.py",
    f"{PROJECT_NAME}/entity/artifact_entity.py",

    f"{PROJECT_NAME}/data_access/__init__.py",
    f"{PROJECT_NAME}/data_access/s3_loader.py",
    f"{PROJECT_NAME}/data_access/redshift_loader.py",


    f"{PROJECT_NAME}/streaming/__init__.py",

    # Kafka
    f"{PROJECT_NAME}/streaming/kafka/__init__.py",
    f"{PROJECT_NAME}/streaming/kafka/producer.py",
    f"{PROJECT_NAME}/streaming/kafka/consumer.py",
    f"{PROJECT_NAME}/streaming/kafka/schema_registry.py",


    f"{PROJECT_NAME}/streaming/spark/__init__.py",
    f"{PROJECT_NAME}/streaming/spark/stream_processor.py",
    f"{PROJECT_NAME}/streaming/spark/watermarking.py",
    f"{PROJECT_NAME}/streaming/spark/exactly_once.py",


    f"{PROJECT_NAME}/feature_store/__init__.py",
    f"{PROJECT_NAME}/feature_store/transaction_features.py",

    f"{PROJECT_NAME}/feature_store/feast_repo/__init__.py",
    f"{PROJECT_NAME}/feature_store/feast_repo/entities.py",
    f"{PROJECT_NAME}/feature_store/feast_repo/feature_views.py",
    f"{PROJECT_NAME}/feature_store/feast_repo/feature_store.yaml",


    f"{PROJECT_NAME}/components/__init__.py",
    f"{PROJECT_NAME}/components/data_ingestion.py",
    f"{PROJECT_NAME}/components/data_validation.py",
    f"{PROJECT_NAME}/components/data_transformation.py",
    f"{PROJECT_NAME}/components/model_trainer.py",
    f"{PROJECT_NAME}/components/model_evaluation.py",
    f"{PROJECT_NAME}/components/model_pusher.py",


    f"{PROJECT_NAME}/pipeline/__init__.py",
    f"{PROJECT_NAME}/pipeline/training_pipeline.py",
    f"{PROJECT_NAME}/pipeline/prediction_pipeline.py",
    f"{PROJECT_NAME}/pipeline/streaming_pipeline.py",


    f"{PROJECT_NAME}/monitoring/__init__.py",
    f"{PROJECT_NAME}/monitoring/data_quality.py",
    f"{PROJECT_NAME}/monitoring/drift_detection.py",
    f"{PROJECT_NAME}/monitoring/latency_metrics.py",


    "airflow/dags/streaming_healthcheck_dag.py",
    "airflow/dags/feature_materialization_dag.py",
    "airflow/dags/model_retraining_dag.py",

    "dashboard/app.py",


    "notebooks/01_data_understanding.ipynb",
    "notebooks/02_eda.ipynb",
    "notebooks/03_feature_analysis.ipynb",
    "notebooks/04_model_training.ipynb",

    "config/schema.yaml",
    "config/model.yaml",
    "config/kafka.yaml",
    "config/spark.yaml",


    "Dockerfile",
    "docker-compose.yml",
    ".dockerignore",
    ".github/workflows/ci.yml",

 
    "requirements.txt",
    "setup.py",
    "pyproject.toml",
    "README.md",
    ".env"
]

def create_structure():
    for file in FILES:
        path = Path(file)
        directory = path.parent

        if directory != Path(""):
            os.makedirs(directory, exist_ok=True)

        if not path.exists():
            with open(path, "w") as f:
                pass
        else:
            print(f"Already exists: {path}")

if __name__ == "__main__":
    create_structure()
    print("Project structure created successfully")
