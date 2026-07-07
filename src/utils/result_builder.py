import json


def create_success_result(algo_result, aggregated_log):
    return json.dumps({'requestId': aggregated_log['requestId'],
                       'entityId': aggregated_log['entityId'],
                       'statusType': 'algorithmSuccess',
                       'results': algo_result['results'],
                       'algorithmName': aggregated_log['algorithmName']}, sort_keys=True)


def create_error_result(aggregated_log):
    return json.dumps({'serviceName': aggregated_log['serviceName'],
                       'serviceVersion': aggregated_log['serviceVersion'],
                       'algorithmName': aggregated_log['algorithmName'],
                       'message': aggregated_log['exception'],
                       'statusType': 'algorithmError',
                       'entityId': aggregated_log['entityId'],
                       'requestId': aggregated_log['requestId']}, sort_keys=True)
