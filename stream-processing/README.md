# A real time data processing pipeline 

[ files you should avoid ]
The components of this code are :
* socket server : broadcast the received data to all clients .
* logger config : logging configuration
* socket client  : send data to the server and receive data from the server .
[ files you can edit ]
* pre processing : process the data before streaming .
* process stream : play with the incoming stream data .
* create stream : create the stream .

Download data from here . change and place your path accordingly .

[Real time stocks data](https://www.kaggle.com/datasets/ayushkhaire/real-time-stocks-data)

# run the code .
```bash
python3 -m venv streamenv
```
```bash
source streamenv/bin/activate
```
```bash
pip install -r requirements.txt
```
```bash
python pre-process.py
```
```bash
python socket_server.py
```
```bash
python create-stream.py
```
```bash
python process-stream.py
```