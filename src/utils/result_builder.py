import json


def create_success_result(result, aggregated_log):
    return json.dumps({'requestId': aggregated_log['requestId'],
                       'entityId': aggregated_log['entityId'],
                       'statusType': 'processingSuccess',
                       'results': result['results'],
                       'pipelineName': aggregated_log['pipelineName']}, sort_keys=True)


def create_error_result(aggregated_log):
    return json.dumps({'serviceName': aggregated_log['serviceName'],
                       'serviceVersion': aggregated_log['serviceVersion'],
                       'pipelineName': aggregated_log['pipelineName'],
                       'message': aggregated_log['exception'],
                       'statusType': 'processingError',
                       'entityId': aggregated_log['entityId'],
                       'requestId': aggregated_log['requestId']}, sort_keys=True)
