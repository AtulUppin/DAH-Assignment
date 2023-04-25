import threading
import queue

def mapper(q_in, q_out):
    word_counts = {}
    while True:
        data_chunk = q_in.get()
        if data_chunk is None:
            break
        for word in data_chunk.split():
            word_counts[word] = word_counts.get(word, 0) + 1
    q_out.put(word_counts)

def reducer(q_in, q_out):
    word_count_totals = {}
    while True:
        word_counts = q_in.get()
        if word_counts is None:
            break
        for word, count in word_counts.items():
            word_count_totals[word] = word_count_totals.get(word, 0) + count
    q_out.put(word_count_totals)

def mapreduce(data, num_workers):
    q_intermediate = queue.Queue()
    q_result = queue.Queue()
    threads = []
    for i in range(num_workers):
        t = threading.Thread(target=mapper, args=(q_intermediate, q_result))
        threads.append(t)
        t.start()
    for chunk in data.splitlines():
        q_intermediate.put(chunk)
    for i in range(num_workers):
        q_intermediate.put(None)
    for t in threads:
        t.join()
    threads = []
    for i in range(num_workers):
        t = threading.Thread(target=reducer, args=(q_result, q_intermediate))
        threads.append(t)
        t.start()
    for i in range(num_workers):
        q_result.put(None)
    for t in threads:
        t.join()
    word_count_totals = {}
    while not q_intermediate.empty():
        word_counts = q_intermediate.get()
        for word, count in word_counts.items():
            word_count_totals[word] = word_count_totals.get(word, 0) + count
    return word_count_totals

if __name__ == '__main__':
    with open('DAH Assignment/input.txt', 'r') as f:
        data = f.read()
    num_workers = 4
    word_count_totals = mapreduce(data, num_workers)
    for word, count in word_count_totals.items():
        print(word, count)
        
        
        
"""OUTPUT: - 
1. 1
Eligibility: 1
Our 2
online 4
internship 4
business 1
is 4
open 1
to 8
individuals 1
who 1
are 3
at 2
least 1
18 2
years 2
of 7
age 1
or 3
older. 1
If 1
you 6
under 1
old, 1
may 3
use 1
our 5
website 1
only 1
with 1
the 8
involvement 1
a 1
parent 1
legal 1
guardian. 1
2. 1
Payment: 1
Payment 1
for 1
internships 1
must 2
be 4
made 1
in 2
full 1
time 1
registration. 1
We 1
accept 1
various 2
forms 1
payment 2
including 2
credit 1
card, 2
debit 1
PayPal, 1
and 9
other 2
electronic 1
methods. 1
All 1
fees 1
paid 1
non-refundable. 1
3. 1
Registration: 1
You 4
complete 1
registration 2
process 1
participate 1
programs. 1
During 1
process, 1
will 1
required 1
provide 2
certain 1
information, 2
such 1
as 1
your 1
name, 1
email 1
address, 1
information. 1
agree 3
that 3
all 3
not 3
reproduced 1
distributed 1
without 2
prior 2
written 2
consent. 2
5. 1
Confidentiality: 1
internship, 1
but 1
limited 1
trade 1
secrets, 1
confidential 1
intellectual 1
property, 1
disclosed 1
any 1
third 1
party 1
"""
