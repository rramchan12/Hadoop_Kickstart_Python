from mrjob.job import MRJob
from mrjob.step import  MRStep
import re

WORD_REG = re.compile(r"[\w']+")

class word_countMR_withReg_sort(MRJob):


    def steps(self):
        return [
            MRStep(mapper = self.read_words_mapper, reducer= self.read_words_reducer),
            MRStep (mapper= self.reverse_word_count_for_sort, reducer = self.sorted_reducer)


        ]

    def read_words_mapper(self, _ , line):
        words = WORD_REG.findall(line)
        for word in words:
            word = unicode(word, "utf-8", errors="ignore")  # avoids issues in mrjob 5.0
            yield word.lower(), 1

    def read_words_reducer(self,word, count):
        yield word, sum(count)

    def reverse_word_count_for_sort(self,word, sum_count):
        yield '%04d'%int(sum_count), word

    def sorted_reducer(self, sum_count, words):
        for word in words:
            yield (word, sum_count)


if __name__ == '__main__':
    word_countMR_withReg_sort.run()