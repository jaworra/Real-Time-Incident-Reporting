# Real Time Incident reporting tool
Real time incident reporting and passenger transport impacts application for traffic operators
to decrease their assessment and response time. Manage deployment on AWS platform including
flexible client engine & API integration. (Python, HTML5/JS, React/D3/Deck.gl, Postgres) 

## Dependencies
- Python 2.7 (AWS lambda + Jupyter Notebook)
- Javascript (libraries deck.gl/D3/plotly and React)
- Postgres (AWS RDS)
- cloudwatch (AWS)

### Repo structure
```bash
├── README.md          # The top-level README for developers.
├── conf               # Space for credentials
│
├── data
│   ├── raw          # Immutable input data
│   ├── curated      # Data used to develop models
│   ├── models       # trained models
│   ├── model_output # model output
│   └── reporting    # Reports and input to frontend
│
├── docs                # Space for Sphinx documentation
│
│
├── images	        # All the images for site	
│
│
├── scripts 	        # contain all the JavaScript code used to 
│			            # add interactive functionality to your site
│
├── styles 	       # contain the CSS code used to style your content
│			  
│
├── notebooks           # Jupyter notebooks. Naming convention is
│                       # date YYYYMMDD (for ordering),
│                       # the creator's initials, and a short `-` 
│                       # delimited description.
│
├── references          # Data dictionaries, manuals, etc. 
│
├── results             # Final analysis and presentaitons.
│
├── requirements.txt    # The requirements file for reproducing the 
│                       # analysis environment.
│
├── .gitignore          # Avoids uploading data, credentials, 
│                       # outputs, system files etc
│
│
└── src                 # python Source code for use in this project.
    ├── __init__.py     # Makes src a Python module
    │
    ├── utils       # Functions used across the projecta e.g lambda layers
    │   └── remove_accents.py
    │
    ├── data        # Scripts to reading and writing data etc
    │   └── load_data.py
    │
    ├── curated      # Scripts to transform data from raw to 
    |   |            # curated
    │   └── curated_data.py
    │
    ├── processing   # Scripts to turn curated data into 
    |   |            # modelling input
    │   └── process_modelling.py
    │
    ├── modelling   # Scripts to train models and then use 
    |   |           # trained models to make predictions. 
    │   └── train_model.py
    │
    ├── model_evaluation   # Scripts that analyse model 
    |   |                  # performance and model selection.
    │   └── calculate_performance_metrics.py
    │
    ├── misc    # Scripts that qa and qc 
    |   |                     
    │   └── dates_days.py
    │           
    ├── reporting   # Scripts to produce reporting tables
    │   └── create_rpt_payment_summary.py
    │
    └── visualisation    #  Scripts to create frequently used plots
        └── visualise_patient_journey.py
```
