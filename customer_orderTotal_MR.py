from mrjob.job import MRJob
from mrjob.step import MRStep


class customer_orderTotalMR(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.group_order_by_customer_mapper, reducer=self.total_order_reducer),
            MRStep(mapper=self.reverse_for_sort, reducer=self.dereverse_the_sort)

        ]



    def group_order_by_customer_mapper(self,_,line):
       customer_id,item,order_amt = line.split(',')
       yield customer_id, float(order_amt)


    def total_order_reducer(self, customer_id, order_amt):
        yield customer_id, sum(order_amt)


    def reverse_for_sort(self, customer_id, order_amt):
        yield '%04.02f'%float(order_amt), customer_id

    def dereverse_the_sort(self, order_amt, customer_id):
        'Even though we write this line, as the customer_id was only a list, but there will only be one value'
        for customer in customer_id:
            yield customer, order_amt


if __name__=='__main__':
    customer_orderTotalMR.run()