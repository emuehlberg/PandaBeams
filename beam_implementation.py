import apache_beam as beam

def run():
    pass

pipe = beam.Pipeline()
data = ( 
    pipe
    | beam.io.ReadFromText('dataset2.csv')
    | beam.Map(print)
)

pipe.run()