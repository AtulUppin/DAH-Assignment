if __name__ == '__main__':
    with open('DAH Assignment/input.txt', 'r') as f:
        data = f.read()
    num_workers = 4
    word_count_totals = mapreduce(data, num_workers)
    for word, count in word_count_totals.items():
        print(word, count)
