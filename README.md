# Airflow ML Pipeline on AWS with Great Expectations and OpenAI Integration 🚀

This project demonstrates a production-grade machine learning pipeline orchestrated with **Apache Airflow**, featuring:
- **Data Quality Checks** via [Great Expectations](https://greatexpectations.io/)
- **Dynamic DAG generation** with Jinja2 templates
- **ML model training/evaluation** for ride duration prediction
- **Conditional model deployment** logic using `BranchPythonOperator`
- **AWS S3** as the data lake for input/output data

---

##  Pipeline Objective

Estimate ride durations for three fictional Mobility-as-a-Service vendors:
- `Easy Destiny`
- `Alitran`
- `ToMyPlaceAI`

Each vendor has separate training data, unique model performance thresholds, and custom deployment behavior. This pipeline:
- Validates input data quality
- Trains a linear regression model
- Evaluates model RMSE
- Deploys the model (or not) based on evaluation results

---

## 🧱 Architecture

```text
             +------------+
             |  Start     |
             +------------+
                    |
           +------------------+
           | Data Quality     | <-- Great Expectations
           +------------------+
                    |
           +------------------+
           | Train & Evaluate | <-- Linear Regression (scipy)
           +------------------+
                    |
           +-------------------+
           | Is Deployable?    | <-- RMSE threshold < 500
           +-------------------+
              /          \
             v            v
        +--------+   +---------+
        | Deploy |   | Notify  |
        +--------+   +---------+
              \         /
               v       v
              +-----------+
              |   End     |
              +-----------+


🙋‍♂️ Author
Ryan Dear
GitHub | LinkedIn | Email
