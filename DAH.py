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
