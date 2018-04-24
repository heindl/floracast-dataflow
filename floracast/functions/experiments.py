import time
import constants

class Experiments:

    def __init__(self):
        self._experiments = []
        self._evals = {}
    #
    #     for columns in [
    #         ['s2_token_1'],
    #         ['s2_token_2'],
    #         ['s2_token_3'],
    #         ['s2_token_4'],
    #         ['s2_token_5'],
    #         ['s2_token_6'],
    #         ['s2_token_7'],
    #         ['s2_token_8'],
    #         ['s2_token_9'],
    #         ['s2_token_10'],
    #     ]:
    #
    #         self._experiments.append(
    #             {
    #                 'id': len(self._experiments),
    #                 'ts': time.time(),
    #                 'columns': columns,
    #             }
    #         )

        for days in [1,2,3,4,5,6]:

            total_groups = 120/days

            for percent_to_remove in [0, 0.2, 0.4]:
            # for percent_to_remove in [0]:

                groups_to_remove = round(total_groups * percent_to_remove)

                shape = total_groups - groups_to_remove

                # for slice_index in [0, int(groups_to_remove)]:
                for slice_index in [int(groups_to_remove)]:

                    for columns in [
                        [constants.KEY_PRCP],
                    ]:

                        self._experiments.append(
                            {
                                'id': len(self._experiments),
                                'ts': time.time(),
                                'columns': columns,
                                'days': int(days),
                                'days_removed': int(groups_to_remove * days),
                                'slice_index': int(slice_index),
                                'shape': int(shape)
                            }
                        )

    def count(self):
        return len(self._experiments)

    def get(self, i):
        return self._experiments[i]

    def register_eval(self, i, res):

        if i not in self._evals:
            self._evals[i] = {
                'max_accuracy': 0,
                'max_auc': 0,
                # Maybe the best evaluation metric for very unbalanced dataset:
                # https://stackoverflow.com/questions/41371154/how-to-debug-uncannily-accurate-tensorflow-model
                'max_precision_recall': 0,
                'min_average_loss': 1,
                'total_evals': 0,
                'prediction/mean': 0,
                'label/mean': 0,
            }

        self._evals[i]['label/mean'] += res['label/mean']
        self._evals[i]['prediction/mean'] += res['prediction/mean']
        self._evals[i]['total_evals'] += 1

        if self._evals[i]['max_accuracy'] < res['accuracy']:
            self._evals[i]['max_accuracy'] = res['accuracy']

        if self._evals[i]['max_precision_recall'] < res['auc_precision_recall']:
            self._evals[i]['max_precision_recall'] = res['auc_precision_recall']

        if self._evals[i]['max_auc'] < res['auc']:
            self._evals[i]['max_auc'] = res['auc']

        if self._evals[i]['min_average_loss'] > res['average_loss']:
            self._evals[i]['min_average_loss'] = res['average_loss']

    def print_tsv(self):
        for i, e in enumerate(self._experiments):
            print('\t'.join([
                'morel',
                str(e['ts']),
                ','.join(e['columns']),
                str(e['days_removed']),
                str(e['slice_index']),
                str(e['shape']),
                str(e['days']),
                str(int((e['days'] * e['slice_index']) - 120)),
                str(int(e['days']*e['shape'])),
                str(self._evals[i]['max_precision_recall']),
                str(self._evals[i]['max_accuracy']),
                str(self._evals[i]['min_average_loss']),
                str(self._evals[i]['max_auc']),
                str(self._evals[i]['label/mean'] / self._evals[i]['total_evals']),
                str(self._evals[i]['prediction/mean'] / self._evals[i]['total_evals']),
            ]))