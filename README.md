# Efficient client server application for a specified scenario
## Group3
###2nd Connected Mobility Assignment

- run `pip install -r requirements.txt` before starting the appliation

- after starting up the virtual network with `python group3/scenario.py`
start a xterm for sta1 and h1 with `xterm sta1/h1`

- on the respective terminal:
    - h1: `python -m group3.server.efficient_server`
    - sta1: `python -m group3.client.efficient_client`
    
The client currently always loads the index.html