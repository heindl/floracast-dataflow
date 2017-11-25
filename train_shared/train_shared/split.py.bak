import tensorflow as tf

flags = tf.app.flags
FLAGS = flags.FLAGS

flags.DEFINE_string("records", "", "Initial records file path")
flags.DEFINE_string("train", "", "Train data file path")
flags.DEFINE_string("eval", "", "Eval data file path ")

def split():

    trainWriter = tf.python_io.TFRecordWriter(FLAGS.train)
    evalWriter = tf.python_io.TFRecordWriter(FLAGS.eval)

    record_iterator = tf.python_io.tf_record_iterator(FLAGS.records)

    randomWrites = 0
    occurrenceWrites = 0

    for record in record_iterator:
        example = tf.train.SequenceExample()
        example.ParseFromString(record)
        label = example.context.feature["label"].int64_list.value[0]
        if label == 0 and randomWrites < 50:
            evalWriter.write(record)
            randomWrites += 1
            continue
        if label != 0 and occurrenceWrites < 50:
            evalWriter.write(record)
            occurrenceWrites += 1
            continue
        trainWriter.write(record)

    trainWriter.close()
    evalWriter.close()

split()