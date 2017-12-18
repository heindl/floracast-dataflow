import tensorflow as tf
import train_shared_input_fn
import functools
import time

train_input_fn = functools.partial(train_shared_input_fn.transformed_input_fn,
                  transformed_location="/tmp/floracast-datamining/transformed/53713/1513571852",
                  batch_size=10,
                  mode=tf.estimator.ModeKeys.TRAIN,
                                   epochs=1)

# for x in range(0, 20):
x, y = train_input_fn()

init_op = tf.global_variables_initializer()
# tf.reset_default_graph()
num_examples = 0
with tf.Session() as session:

    session.run(tf.global_variables_initializer())
    session.run(tf.local_variables_initializer())

    coord = tf.train.Coordinator()
    threads = tf.train.start_queue_runners(coord=coord, sess=session)

    try:
        step = 0
        while not coord.should_stop():
            start_time = time.time()
            e, l = session.run([x, y])
            num_examples = num_examples + l.shape[0]
            print(l)
            print(e)
            # e, l = session.run([x, y])
            # num_examples = num_examples + l.shape[0]
            # print("num_examples = " + str(num_examples))
            # duration = time.time() - start_time

    except tf.errors.OutOfRangeError:
        print('Done training')
    finally:
        # When done, ask the threads to stop.
        coord.request_stop()

        # Wait for threads to finish.
        coord.join(threads)
        session.close()
print(num_examples)