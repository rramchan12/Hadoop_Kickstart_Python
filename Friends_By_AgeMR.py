from mrjob.job import MRJob

class MRFriendsByAge(MRJob):
    def mapper(self, _ , line):
        (ID,name, age, numFriends) = line.split(',')
        print ("Line",ID,name,age,numFriends)
        yield age, float(numFriends)



    def reducer(self, age, numFriends):
        total = 0
        numTimesTheAgeEncountered = 0;
        for x in numFriends:
            total += x
            numTimesTheAgeEncountered += 1
        'Now the avg = total/no of times age was encountered'
        yield age, total/numTimesTheAgeEncountered


if __name__ == '__main__':
    MRFriendsByAge.run()

