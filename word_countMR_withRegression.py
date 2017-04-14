from mrjob.job import MRJob
import re

REG_EXP = re.compile(r"[\w']+")

class word_counterMR_reg(MRJob):

    def mapper(self, _, line):
        words = REG_EXP.findall(line)
        for word in words:
            word = unicode(word,"utf-8", errors="ignore")  #unicode problems in mrjob 5
            yield (word,1)


    def reducer(self, word, count):
        yield (word, sum(count))


if __name__ == '__main__':
    word_counterMR_reg.run()

