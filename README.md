## Real Time Taxi Monitoring Engine

This project provides real-time monitoring and analysis of a fleet of 10,000+ taxis, displaying and updating live data including average speed, current speed, location, and timestamps. As the dashboard is fed live data, the system is capable of notifying and storing information.  The dashboard is designed to help fleet managers and operators efficiently track and manage their vehicles.

## Installation
If you already have the necessary tools installed, you can skip step 1.

### Step 1: Prerequisites

You need the following programs/tools:

1. **Git Bash Terminal** - Follow the steps provided [here] https://git-scm.com/downloads .
2. **Docker** - Follow the installation instructions [here] https://docs.docker.com/engine/install/.

### Step 2: Open Git Bash Terminal

1. **Navigate to the location where you want to set up your project**:
    ```bash
    cd PATH_TO_YOUR_WORKSPACE
    ```

2. **Clone the project**:
    ```bash
    git clone https://collaborating.tuhh.de/e-19/teaching/bd24_project_m8_b.git
    ```

## Usage
Before proceeding, ensure that Docker is running.

### Step 1: Start the Project

1. **Open Git Bash Terminal and navigate to the folder where you cloned the project**:
    ```sh
    cd PATH_TO_THE_PROJECT
    ```

2. **Run the following command**:
    ```sh
    docker-compose up -d
    ```
    > Starting with Docker Compose v2 (or later versions), you can omit the hyphen and use docker compose up -d instead of docker-compose up -d.

### Step 2: Open the Project

1. **Open your preferred browser**.

2. **Go to the following URL to view the dashboard**:
    ```sql
   localhost:5601/app/dashboards#/view/aea780f0-1ee2-11ef-a2ca-172534fb526f?embed=true&_g=(refreshInterval:(pause:!f,value:5000),time:(from:now-7d%2Fd,to:now))&hide-filter-bar=true
    ```
    
### Step 3: Close the project

1. **Inside the same gitbash Terminal run the following command**
    ```sh
    docker compose down
    ```

## License
This project is licensed under the MIT License. You are free to use, modify, and distribute this software as long as you include the original copyright and license notice in any copy of the software you create or distribute.

**MIT License**
