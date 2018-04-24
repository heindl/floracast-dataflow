import tensorflow as tf
import train

training_data = train.TrainingData(
    project="floracast-firestore",
    gcs_bucket="floracast-datamining",
    name_usage_id="qWlT2bh",
    train_batch_size=20,
    train_epochs=1,
    transform_data_path='/tmp/zwTPA2M1I',
)

eval_input_fn, train_input_fn = training_data.input_functions(0.05)

with tf.Graph().as_default():
    x, y = eval_input_fn()
    global_vars = tf.global_variables_initializer()
    local_vars = tf.local_variables_initializer()
    table_vars = tf.tables_initializer()
    with tf.Session().as_default() as session:
        session.run(global_vars)
        session.run(local_vars)
        session.run(table_vars)

        tf.train.start_queue_runners()

        y, x = session.run([y, x['max_temp']])
        for i, b in enumerate(y):
            if b:
                print(i)
                print(x[i])