from mrjob.job import MRJob
from mrjob.step import MRStep

class max_ratedMR_2_reducers(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.load_movie_mapper, reducer = self.movie_rating_reducer),
            MRStep(mapper=self.pass_through_mapper, reducer = self.find_max)
        ]


    def load_movie_mapper(self, _, line):
        (user_id,movie_id,rating,timestamp) = line.split('\t')
        yield (movie_id, 1)

    def movie_rating_reducer(self, movie_id, count):
    #You need a tuple like that as this is how max works
        yield  None, (sum(count), movie_id)

    # This mapper does nothing; it's just here to avoid a bug in some
    # versions of mrjob related to "non-script steps." Normally this
    # wouldn't be needed.
    def pass_through_mapper(self, key, value):
        yield key, value

    def find_max(self, key, value):
        yield max(value)


if __name__ == '__main__':
    max_ratedMR_2_reducers.run()

