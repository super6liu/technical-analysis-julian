#!/bin/bash
red=`tput setaf 1`
green=`tput setaf 2`
reset=`tput sgr0`

echo "${green}Setting up dev env..."
echo "${reset}"

mkdir -p logs

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
deactivate

echo "${green}Done."
echo "${reset}"
