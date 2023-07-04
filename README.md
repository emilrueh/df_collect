# ValueRadar Data Aggregator

## Project Description

The ValueRadar Data Aggregator is a Python-based solution designed to automate the collection and transformation of event data from multiple sources. This data, once cleaned and standardized, is then pushed to a Xano backend. This streamlined approach enables the rapid processing of data, eliminating manual intervention and significantly improving efficiency.

This is an early-stage project and is intended more as a showcase than a typical open-source project. The codebase is rapidly evolving, so check back often to see the latest developments!

## Core Component

The script `xano_scripts.py` sends the cleaned and transformed data to Xano, a backend as a service platform. In addition to sending new data, it can also check existing records in Xano, update them if necessary, and archive or delete old events. 

There is also a helper script, `data_scripts.py`, which can perform data manipulation tasks like adding or removing columns, changing text case, splitting a single column into multiple ones, and filtering out rows based on certain keywords.

## Getting Started

To use the ValueRadar Data Aggregator, follow the steps below:

### Pre-requisites

- Python 3.6 or higher installed on your system.
- Access to internet (for installing dependencies).

### Installation

1. **Clone the repository**: Use the command `git clone <repository_url>` in your terminal to download the project files onto your local system.

2. **Install Dependencies**: Navigate to the project directory in your terminal and run the command `pip install -r requirements.txt`. This will install the Python libraries that the project depends on.

### Setting Up Your Environment

This project uses environment variables for configuration to keep sensitive data like API keys secure. These are stored in a file named `.env` in the project root directory. You will need to create this file and populate it with your configuration information to run the project.

### Running the Script

Once your `.env` file is set up, you can run the script with the following command: `python main.py`

This will initiate the data aggregation process - scraping data from specified sources, transforming it, and sending it to your Xano backend. To understand how the script works, take a look at `main.py`. The comments in this file provide guidance on the flow of the program and how each function operates.

## Observations and Next Steps

As this project is currently in its early stages, it is primarily designed to showcase the potential of automated data aggregation. It's not fully open for contributions in the traditional open-source manner, but we're always eager to hear your feedback and suggestions. 

The future roadmap of this project includes significant expansion in terms of its feature set, data sources, and overall functionality. Stay tuned!

## License

This project serves as a demonstration of and it is not intended for cloning or external contributions. We kindly ask that you respect this intention by not using it for commercial purposes or distributing it. 

This work is licensed under the Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License. This means that while you are welcome to view and understand the code, it is not meant to be repurposed under standard open-source protocols. 

To view a copy of this license, please visit [http://creativecommons.org/licenses/by-nc-nd/4.0/](http://creativecommons.org/licenses/by-nc-nd/4.0/) or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.

The terms of this license stipulate that:
- **Attribution** - If sharing the material, you must give appropriate credit and provide a link to the license.
- **NonCommercial** - The material may not be used for commercial purposes.
- **NoDerivatives** - If you remix, transform, or build upon the material, you may not distribute the modified material.
