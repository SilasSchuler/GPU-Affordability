# GPU Affordability
DENG Module Group Project

GPU prices are rising rapidly, leaving many consumers uncertain about when to buy their preferred hardware. This issue is compounded by constant news coverage and frequent releases of new Large Language Models (LLMs).


## Use Case

This project provides a forecast to help small businesses and individuals look past the hype. It helps determine whether an intended GPU can be purchased at a reasonable price now, or if waiting is the better option.

To achieve this, the project maintains a daily-updated table ranking GPUs into two categories: **Buy Now** vs. **Wait**.


## Data

- **Historical Hardware Deals:** A dataset tracking hardware deals since last year.
  - **Source:** [HardwareDealsCo/gpu-deals](https://github.com/HardwareDealsCo/gpu-deals)
  - **Details:** The source scrapes deal data from the website HardwareDeals.co, which compiles hardware listings from eBay.

- **AI Release Milestones:** An additional dataset collecting major AI model releases and company announcements.
  - **Details:** Gathered manually (or via scraping where feasible) to enhance the model's forecasting performance.


## Architecture

![Architecture v0.1](images/architecture_v0.1.png)




## UV Environment

UV is generous on both Windows and Mac devices, which makes it more generous for a group project.

```
uv init

# On every update.
uv sync

# Add a new library to the project.
uv add {library}

# Run python scripts
uv run {script}
```


### Initial Plan

#### W3 Pitch + Initial Project Setup
- Data source

#### W7 Midterm
- Ingestion pipeline (4); 
- local storage/schema (2); 
- Docker/reproducible environment (3); 
- ReadMe with instructions required for Peer Reproducibility
- Proper Review
- Oral Defence (Understand entire codebase and decisions. Both team members.)

Silas:
- Local Storage 
- Docker environment

Natalie:
- Ingestion

Shared:
- Reproducibility
- ReadMe


#### W14
- Terraform/cloud infrastructure (3); 
- cloud ingestion (3); 
- transformation and warehouse design (3); 
- orchestration, reliability and data quality (2); 
- documentation, security and reproducibility (2):
- Peer Review
- Oral Defence (Understand entire codebase and decisions. Both team members.)

Silas:
- transformation and warehouse design
- cloud ingestion


Natalie:
- Terraform/cloud infrastructure
- orchestration, reliability and data quality

Shared:
- documentation, security and reproducibility

Optional: Compare classification with prognosis model with prediction for next day. 