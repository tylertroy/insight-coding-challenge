import codecs
import collections
import bisect
import re
import sys


class Antifraud(object):
    """ A payment verification class for the Paymo digital wallet."""
    def __init__(self, argv):
        self.network = Network('Paymo')
        
        self.batch_path = argv[1]
        self.stream_path = argv[2]
        self.output1_path = argv[3]
        self.output2_path = argv[4]
        self.output3_path = argv[5]

        self.verified = 'trusted\n'
        self.unverified = 'unverified\n'

    def load_transactions(self):
        """Load batch data from self.batch_payment path."""
        self.date_regex = r'\d+-\d+-\d+\s\d+:\d+:\d+'
        pattern = re.compile(self.date_regex)
        with codecs.open(self.batch_path, encoding='utf-8') as f:
            self.batch_header = next(f) # skip header
            transactions = [ line.split(',', 4)[1:3] 
                for line in f if re.match(pattern, line) ]
        transactions = [ element for element in transactions if element != [] ]
        transactions = [ (int(id1), int(id2)) for (id1, id2) in transactions ]
        self.transactions = transactions

    def build_network_from_batch(self):
        """ Add persons to network from bacth data."""
        for (person1, person2) in self.transactions:
            self.network.add_person(Person(person1))
            self.network.add_person(Person(person2))
            self.network.register_friendship(person1, person2)

    def test_stream_data(self):
        """Load stream data from stream data and check line-by-line."""
        with codecs.open(self.stream_path, 'r', encoding='utf-8') as f, \
                    open(self.output1_path, 'w') as output1, \
                    open(self.output2_path, 'w') as output2, \
                    open(self.output3_path, 'w') as output3:
        
            outputs = (output1, output2, output3)
            degrees = (1, 2, 4)

            for transaction in f:
                try:
                    transaction = transaction.split(',', 4)
                    id1, id2 = int(transaction[1]), int(transaction[2])
                except (IndexError, ValueError, UnicodeEncodeError):
                    continue
                
                for output, degree in zip(outputs, degrees):
                    if self.network.find_degree(id1, id2, degree):
                        output.write(self.verified)
                    else:
                        output.write(self.unverified)

    def run(self):
        self.load_transactions()
        self.build_network_from_batch()
        self.test_stream_data()


class Person(object):
    """ A person object generated in the network."""
    _infinity = float('inf')

    def __init__(self, person_id):
        """Create a new person in with an empty set of friends."""
        self.id = person_id
        self.friends = []

        self.degree = self._infinity
        self.checked = False

    def add_friend(self, person_id):
        """Add a new friend to the persons friendship network."""
        if person_id not in self.friends:
            bisect.insort(self.friends, person_id)

    def reinitialize(self):
        self.degree = self._infinity
        self.checked = False


class Network(object):
    """A paymo friendship Network."""
    def __init__(self, name):
        self.name = name
        self.members = {}
        self.infinity = float('inf')

    def add_person(self, person):
        """Add a person to the network."""
        if isinstance(person, Person) and person.id not in self.members:
            self.members[person.id] = person
            return True
        else:
            return False

    def register_friendship(self, id1, id2):
        """Make a connection or friendship between two members."""
        if id1 in self.members and id2 in self.members:
            self.members[id1].add_friend(id2)
            self.members[id2].add_friend(id1)
            return True
        else:
            return False

    def reinitialize_persons(self):
        """ Reinitialize checked and order  for person objects."""
        for person in self.members.values():
            person.reinitialize()

    def find_degree(self, id1, id2, degree):
        """Find the friendship order for the friends of a friend.""" 
        if id1 in self.members[id2].friends: 
                return True

        queue = collections.deque()
        person = self.members[id1] 
        person.degree = 0
        person.checked = True
        for friend_id in person.friends:
            self.members[friend_id].degree = person.degree + 1
            queue.append(friend_id)

        while queue:
            friend_id = queue.popleft()
            friend = self.members[friend_id]
            friend.checked = True

            for their_friend_id in friend.friends:
                their_friend = self.members[their_friend_id]
                if not their_friend.checked:
                    queue.append(their_friend_id)
                    if their_friend.degree > friend.degree + 1:
                        their_friend.degree = friend.degree + 1
                if their_friend_id == id2:  
                    if their_friend.degree <= degree:
                        return True

        self.reinitialize_persons()
        return False

    def show_graph(self):
        """Print the state of the degree find.
        used for debugging purposes.
        """
        for person_id in sorted(list(self.members.keys())):
            print('{}{} {}'.format(
                person_id, 
                self.members[person_id].friends, 
                self.members[person_id].degree,
            ))

if __name__ == '__main__':

    argv = sys.argv
    antifraud = Antifraud(argv)
    antifraud.run()
    quit()
