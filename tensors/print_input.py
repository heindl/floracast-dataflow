from main import get_input_fn, parse_file
import tensorflow as tf


file = "./train.tfrecords"

# features = parse_file(file, tf.contrib.learn.ModeKeys.TRAIN)
#

# print(elevation)



fn = get_input_fn(file, tf.contrib.learn.ModeKeys.TRAIN, 5)
x, y = fn()

ran = tf.contrib.learn.run_n({
    'x': x['x'],
    'y': y
}, n=1, feed_dict=None)

print(ran)

# batch_mean, batch_var = tf.nn.moments(x['elevation'], [0])
# variation = tf.contrib.learn.run_n({"variation": batch_var}, n=1, feed_dict=None)
# print(variation)



