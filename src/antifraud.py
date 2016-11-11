import os
import sys
import numpy as np
import codecs

class Antifraud(object):
    """ Confirms the relationship between transaction parties. """
    def __init__(self, argv):
        self.batch_path = argv[1]
        self.stream_path = argv[2]
        self.output1_path = argv[3]
        self.output2_path = argv[4]
        self.output3_path = argv[5]
        self.network = {}
        self.run()

    def load_network_data(self):
        """ Load inital network state. """
        names = ('time', 'id1', 'id2', 'amount', 'message')
        transactions = pd.read_csv(self.batch_path, 
            skiprows=1,
            names=names,
            encoding='utf-8',
            error_bad_lines=False,
            )
        t = transactions
        t = t[t['time'].astype(str).str.startswith('2016')]
        t = t[['id1','id2']].astype(int)
        self.transactions = t[['id1','id2']].values
    
    def load_network_state(self):
        """ Load the data from batch_payment"""
        with codecs.open(self.batch_path, encoding='utf-8') as f:
            next(f) # skip header
            transactions = [ line.split(',', 4)[1:3] for line in f ]
            transactions = [ element for element in transactions if element != [] ]
            transactions = np.array(transactions).astype(int)
        self.users = np.unique(transactions)
        self.transactions = transactions

    def make_network(self):
        """ Generate friendship network from batch data. """
        self.network = { person : {person} for person in np.unique(self.transactions) }
        for payer, payee in self.transactions:
            self.network[payer].add(payee)
            self.network[payee].add(payer)


    def output1_test(self, payee, payer):
        """ Return trusted if payer and payee are in network. """
        if payee in self.network[payer]:
            return True
        return False

    def output2_test(self, payee, payer):
        """ Return trusted if payer and payee have a mutual friend. """
        if self.network[payee] & self.network[payer]:
            return True
        return False

    def output3_test(self, payee, payer):
        """ Return trusted if payer and payee are connected to the 4th order. """
        return False

    def write_output1(self):
        """ Read stream data and write to output1.txt """
        output = open(self.output1_path, 'w')
        with codecs.open(self.stream_path, 'r', encoding='utf-8') as f:
            for transaction in f:
                try:
                    transaction = transaction.split(',', 4)
                    payee, payer = np.array(transaction[1:3]).astype(int)
                    if self.output1_test(payee, payer):
                        test = 'trusted\n'
                    else:
                        test = 'unverified\n'
                    output.write(test)
                except:
                    pass
        output.close()

    def write_output2(self):
        """ Read stream data and write to output1.txt """
        output = open(self.output2_path, 'w')
        with codecs.open(self.stream_path, 'r', encoding='utf-8') as f:
            for transaction in f:
                try:
                    transaction = transaction.split(',', 4)
                    payee, payer = np.array(transaction[1:3]).astype(int)
                    if self.output1_test(payee, payer):
                        test = 'trusted\n'
                    if self.output2_test(payee, payer):
                        test = 'trusted\n'
                    else:
                        test = 'unverified\n'
                    output.write(test)
                except:
                    pass
        output.close()

    def write_output(self):
        output1 = open(self.output1_path, 'w')
        output2 = open(self.output2_path, 'w')
        output3 = open(self.output3_path, 'w')
        with codecs.open(self.stream_path, 'r', encoding='utf-8') as f:
            for transaction in f:
                try:
                    transaction = transaction.split(',', 4)
                    payee, payer = np.array(transaction[1:3]).astype(int)

                    
                    if self.output1_test(payee, payer):
                        output1.write('trusted\n')
                        output2.write('trusted\n')
                    else: 
                        output1.write('unverified\n')
                        if self.output2_test(payee, payer):
                            output2.write('trusted\n')
                        else:
                            output2.write('unverified\n')
                except:
                    pass
        output3.write("So close! Just plumb ran out of time.")
        output1.close()
        output2.close()
        output3.close()

    def run(self):
        """ Combine the functions to test the data. """
        self.load_network_state()
        self.make_network()
        self.write_output()

if __name__ == '__main__':
    antifraud = Antifraud(sys.argv)
    quit()
