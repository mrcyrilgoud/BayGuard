# BayWatch

## Overview
Environmental compliance detection system for monitoring air and water quality. <br>
Collects data from tha EPA's [Air Quality System (AQS) API](https://aqs.epa.gov/aqsweb/documents/data_api.html) and generated alerts for compliance violations. 

## Installation

### Linux
#### 1. Install Git and download the repository
```
sudo apt install git
```
Check git version to verify installation
```
git --version
```
Clone the repository
```
git clone https://github.com/mrcyrilgoud/BayGuard.git
```
Change working directory
```
cd BayGuard
```

#### 2. Get API key
Follow [signup instructions](https://aqs.epa.gov/aqsweb/documents/data_api.html#signup) to get API key

Save API key to a .env file
```
AQS_API_KEY=<your api key>
EMAIL=<your email>
```

#### 3. Install and run docker
```
sudo snap install docker
```
Check docker version to verify installation
```
docker --version
```
Run docker compose
```
docker-compose up -d
```
Check docker containers are running
```
docker ps -a
```

#### 4. Run producer and consumer
TODO

## Technologies and Tools
* [Git](https://git-scm.com/) // version control
* [Visual Studio Code](https://code.visualstudio.com/) // IDE
* [Apache Kafka](https://kafka.apache.org/) // event handling
* [Docker](https://www.docker.com/) // deployment

## Acknowledgement
Created by [Cyril Goud](https://github.com/mrcyrilgoud), [Albert Ong](https://github.com/Albert-C-Ong), and [David Thach](https://github.com/cloddity)