import tensorflow as tf
import model
import input_fn

run_config = tf.contrib.learn.RunConfig()
run_config = run_config.replace(model_dir="/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/models/1505912563")

estimator = model.get_estimator(run_config)

for p in estimator.predict(
        input_fn=input_fn.get_test_prediction_data_fn({
            "train_data_path": "/Users/m/Desktop/phenograph/infra/src/bitbucket.org/heindl/dataflow/gs/floracast-models/train/1505856591"
        }, "/Users/m/Downloads/forests-data.tfrecords"),
        # as_iterable=False
):
    print(p)