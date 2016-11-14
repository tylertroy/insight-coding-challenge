# insight-coding-challenge
a digital wallet "paymo" payment verification suite

## Requirements

* python 3.4.3

## Usage

Run from the command line as follows.

```bash
python3 ./src/antifraud.py ./paymo_input/batch_payment.txt ./paymo_input/stream_payment.txt ./paymo_output/output1.txt ./paymo_output/output2.txt ./paymo_output/output3.txt
```

N.B. not tested with other python versions. 

## Details of Implimentation

`antifraud.py` consists of 3 classes:

* Antifraud
* Person
* Network

`Antifraud` is the controller class generating instances of `Network` and `Person`, and operating the operation of those objects. It also handles the file input and ouput, as well as testing of the `stream_payment.txt` lines.


