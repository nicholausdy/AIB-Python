#!/bin/bash

pm2 start --name=python-app -i 1 app/event_listener.py --interpreter="./bin/python3"
