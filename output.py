import multiprocessing

from DAH Assignment.DAH import mapreduce


if __name__ == '__main__':
    with open('C:/Users/ATUL/OneDrive/Desktop/My  Projects/DAH Assignment/input file.txt', 'r') as f:
        data = f.read()
    
    num_workers = multiprocessing.cpu_count()
    word_count_totals = mapreduce(data, num_workers)
    
    print(word_count_totals)