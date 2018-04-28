import tensorflow as tf
import train
import occurrences
import constants

name_usage_id = "qWlT2bh"

occurrence_records = occurrences.OccurrenceTFRecords(
    name_usage_id=name_usage_id,
    project="floracast-firestore",
    gcs_bucket="floracast-datamining",
    occurrence_path="/tmp/occurrence-data-keep/occurrences/",
    random_path="/tmp/occurrence-data-keep/random/",
    multiplier_of_random_to_occurrences=1,
    test_train_split_percentage=0.1,
)

# print("Total Experiments", exp.count())

# for experiment_number in range(exp.count()):

training_data = train.TrainingData(
    project="floracast-firestore",
    gcs_bucket="floracast-datamining",
    name_usage_id=name_usage_id,
    train_batch_size=16,
    train_epochs=1,
    occurrence_records=occurrence_records,
    transform_data_path='/tmp/transform-data-keep',
    # experiment=exp.get(experiment_number)
    # model_path='/tmp/SQtjlLHyi/model'
)

eval_input_fn, train_input_fn = training_data.input_functions()

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

        # for k in x.keys():
        print(session.run([x[constants.KEY_TEMP_DIFFERENCE]]))


        # for i, b in enumerate(y):
        #     if b:
        #         print(i)
        #         print(x[i])