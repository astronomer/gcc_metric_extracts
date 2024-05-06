# Overview
Extract Cloud Composer utilization metrics and map them to expected Astronomer resources

# Setup
## Authentication
Ensure that you are [authenticated](https://cloud.google.com/docs/authentication/gcloud) with Google Cloud and that the [Monitoring API is enabled](https://cloud.google.com/monitoring/api/enable-api#enabling-api-v3)

## Virtual Environent
Requires python >= 3.11. Create a virtual environment, activate it an install the requirements

### Linux/Mac
```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Windows

# Usage
Run the `extract_metrics.py` file with the command line options corresponding to your GCC environment. To view 
```
python extract_metrics.py --help
```


