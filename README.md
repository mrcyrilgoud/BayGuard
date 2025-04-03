# BayWatch

## Overview
Environmental compliance detection system

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
#### 2. Install and run docker
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

#### 3. Get API key
Follow instructions to get API key<br>
https://aqs.epa.gov/aqsweb/documents/data_api.html#signup

Save API key to .env file
```
API_KEY=<your api key>
EMAIL=<your email>
```

TODO


## Acknowledgement
Created by [Cyril Goud](https://github.com/mrcyrilgoud), [Albert Ong](https://github.com/Albert-C-Ong), and [David Thach](https://github.com/cloddity)