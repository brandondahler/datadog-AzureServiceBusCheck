import os
import sys
import calendar
from datetime import datetime, timedelta
from tempfile import mkstemp

from checks import AgentCheck

# Hack to include required .pyd modules
if len(sys.path) == 1:
    path_string = sys.path[0]

    if path_string.endswith('.zip'):
        sys.path.append(path_string[:-4])

from azure.servicemanagement import ServiceBusManagementService, get_certificate_from_publish_settings
from rfc3339 import datetimetostr, tzinfo


class AzureServiceBusCheck(AgentCheck):
    def check(self, instance):
        subscription_id, cert_file_handle, cert_file_path, namespace, tags = AzureServiceBusCheck._load_conf(instance)
        tags.append("namespace:" + namespace)
        tags.append("subscription:" + subscription_id)

        management_service = ServiceBusManagementService(subscription_id, cert_file_path)
        queues = management_service.list_queues(namespace)

        rate_base_datetime = datetime.utcnow() - timedelta(minutes=15)
        rate_datetime = rate_base_datetime \
            .replace(minute=rate_base_datetime.minute - rate_base_datetime.minute % 5,
                     second=0,
                     microsecond=0,
                     tzinfo=tzinfo(0, 'Z'))

        rate_datetime_timestamp = calendar.timegm(rate_datetime.utctimetuple())

        metric_filter = AzureServiceBusCheck.get_metric_filter(rate_datetime, timedelta(minutes=5))

        for queue in queues:  # type: QueueDescription
            queue_tags = list(tags)
            queue_tags.append("queue:" + queue.name)

            self.check_queue(queue, queue_tags)

            metrics = management_service.get_supported_metrics_queue(namespace, queue.name)  # type: MetricProperties
            for metric in metrics:
                metric_data_list = management_service.get_metrics_data_queue(
                    namespace, queue.name,
                    metric.name, 'PT5M',
                    metric_filter)

                self.check_metric(metric, metric_data_list, queue_tags, rate_datetime_timestamp)

        if cert_file_handle is not None:
            os.close(cert_file_handle)

        os.remove(cert_file_path)

    def check_queue(self, queue, queue_tags):
        self.gauge('queue.messages.total', queue.message_count, queue_tags)
        self.gauge('queue.messages.active', queue.count_details.active_message_count, queue_tags)
        self.gauge('queue.messages.scheduled', queue.count_details.scheduled_message_count, queue_tags)
        self.gauge('queue.messages.dead_lettered', queue.count_details.dead_letter_message_count, queue_tags)
        self.gauge('queue.messages.transfer', queue.count_details.transfer_message_count, queue_tags)

        self.gauge('queue.messages.transfer_dead_lettered',
                   queue.count_details.transfer_dead_letter_message_count,
                   queue_tags)

    def check_metric(self, metric, metric_data_list, queue_tags, timestamp):
        aggregated_value = 0

        if len(metric_data_list) > 0:
            metric_data = metric_data_list.pop()

            aggregated_value = metric_data.total

            if metric.primary_aggregation == "Max":
                aggregated_value = metric_data.max

        self.gauge('queue.metrics.%s' % metric.name, aggregated_value,
                   queue_tags, timestamp=timestamp)

    @staticmethod
    def _load_conf(instance):
        if 'publish_settings' in instance:
            cert_file_handle, cert_file_path = mkstemp()
            get_certificate_from_publish_settings(instance.get('publish_settings'), cert_file_path)
        else:
            cert_file_handle = None
            cert_file_path = instance.get('cert_file')

        namespace = instance.get('namespace')

        tags = []
        if 'tags' in instance:
            tags = instance.get('tags')

        return instance.get('subscription_id'), cert_file_handle, cert_file_path, namespace, tags

    @staticmethod
    def get_metric_filter(rate_datetime, window_size):
        rate_datetime_lower_string = datetimetostr(rate_datetime - window_size)
        rate_datetime_upper_string = datetimetostr(rate_datetime)

        return \
            '$filter=Timestamp ge datetime\'%s\' and Timestamp lt datetime\'%s\'' % \
            (rate_datetime_lower_string, rate_datetime_upper_string)


if __name__ == '__main__':
    check, instances = AzureServiceBusCheck.from_yaml('AzureServiceBusCheck.yaml')
    for instance in instances:
        print "\nRunning the check against: %s - %s" % (instance['subscription_id'], instance['namespace'])

        check.check(instance)

        if check.has_events():
            print 'Events: %s' % (check.get_events())

        print 'Metrics: %s' % (check.get_metrics())
