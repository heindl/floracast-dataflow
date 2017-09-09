import tensorflow as tf

flags = tf.app.flags
FLAGS = flags.FLAGS

flags.DEFINE_string("records", "", "Initial records file path")

def count_records():

    record_iterator = tf.python_io.tf_record_iterator(FLAGS.records)
    randoms = 0
    occurrences = 0
    for record in record_iterator:
        # example = tf.train.SequenceExample()
        # example.ParseFromString(record)
        randoms = randoms + 1
        # if example.context.feature["label"].int64_list.value[0] == 0:
        #     randoms += 1
        # else:
        #     occurrences += 1
            # print(example.context.feature["label"].int32_list.value[0])
            # print(example)
            # break

    print("randoms/occurrences", randoms, occurrences)

count_records()