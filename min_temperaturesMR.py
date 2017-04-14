from mrjob.job import MRJob

class MinTemperatures(MRJob):
    def convert_to_farenheit(self, tenthofCelsius):
        celsius = float(tenthofCelsius)/10
        farenheit = celsius * 1.8 +32.0
        return farenheit

    def mapper(self, _ , line):
        (location, date, type, data, x,y,z,w ) = line.split(',')
        if (type == 'TMIN'):
            temperature = self.convert_to_farenheit(data)
            yield (location, temperature)


    def reducer(self, location, temperature):
        yield (location, min(temperature))

if __name__ == '__main__':
    MinTemperatures.run()
