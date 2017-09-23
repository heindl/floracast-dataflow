import tensorflow as tf
import model
import input_fn

def create_parser():
    import argparse
    """Initialize command line parser using arparse.
    Returns:
      An argparse.ArgumentParser.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--train_data_path', type=str, required=True)
    parser.add_argument('--output_path', type=str, required=True)
    parser.add_argument(
        '--hidden_units',
        nargs='*',
        help='List of hidden units per layer. All layers are fully connected. Ex.'
             '`64 32` means first layer has 64 nodes and second one has 32.',
        default=[512],
        type=int)
    parser.add_argument(
        '--model_dir',
        type=str,
        required=True
    )
    parser.add_argument("--raw_data_file_pattern", type=str, required=True)
    return parser


def main(argv=None):
    import input_fn
    import sys, io, json, os
    import model

    argv = sys.argv if argv is None else argv
    args = create_parser().parse_args(args=argv[1:])

    run_config = tf.contrib.learn.RunConfig()
    run_config = run_config.replace(model_dir=args.model_dir)

    estimator = model.get_estimator(args, run_config)

    date = os.path.basename(args.raw_data_file_pattern)

    label_vocabulary = model.get_label_vocabularly(args.train_data_path)

    a = []
    for p in estimator.predict(input_fn=input_fn.get_test_prediction_data_fn(args)):

        probabilities = []

        for prob in p['probabilities']:
            probabilities.append(float(prob))

        a.append({'probabilities': probabilities, 'classes': label_vocabulary, 'key': p['occurrence_id']})

    output_path = os.path.join(args.output_path, ("%s.json" % date))

    with io.open(output_path, 'w', encoding='utf-8') as f:
        f.write(unicode(json.dumps(a, f, ensure_ascii=False)))


if __name__ == '__main__':
    main()