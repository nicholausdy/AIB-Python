# AIB-Python
Repository for AI for Business final assignment. This is the Python 3 component that utilizes asynchronous ML processing function call using Asyncio. Companion repo for the API server is: https://github.com/nicholausdy/AIB-Node

This app receives message from API server, process the request, then send the reply back to the API server
## Dependencies
1. Python 3.6 and Pip 3
2. NPM
3. pm2
4. Git LFS
5. NATS message broker (https://docs.nats.io/nats-server/installation)

## How to Run
Clone the repo
``` 
git clone https://github.com/nicholausdy/AIB-Python
```
Move to the working directory
``` 
cd AIB-Python
```
Create virtual environment
``` 
python3 -m venv .
```
Activate virtual environment
``` 
source bin/activate
```
Install requirements
```
pip3 install -r requirements.txt
```
Create env file at the same directory as .env.example <br>
Run the app!
```
./run.sh
```
