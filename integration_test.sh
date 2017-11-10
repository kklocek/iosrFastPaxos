#!/bin/bash

python3 service_discovery.py &
sleep 2
python3 application.py &
