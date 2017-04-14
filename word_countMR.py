from mrjob.job import MRJob


class word_count(MRJob):
    def mapper(self, _, line):
        words = line.split(' ')
        for word in words:
            yield (word, 1)

    def reducer(self, word, count):
        yield (word, sum(count))


if __name__ == '__main__':
    word_count.run()
