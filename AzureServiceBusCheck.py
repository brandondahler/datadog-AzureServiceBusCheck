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

from azure.servicemanagement import *
from rfc3339 import datetimetostr, tzinfo


class AzureServiceBusCheck(AgentCheck):
    def check(self, instance):
        self.log.info(sys.path)

        subscription_id, cert_file, namespace, tags = AzureServiceBusCheck._load_conf(instance)
        tags.append("namespace:" + namespace)
        tags.append("subscription:" + subscription_id)

        sbms = ServiceBusManagementService(subscription_id, cert_file)
        queues = sbms.list_queues(namespace)

        rate_base_datetime = datetime.utcnow() - timedelta(minutes=15)
        rate_datetime = rate_base_datetime \
            .replace(minute=rate_base_datetime.minute - rate_base_datetime.minute % 5,
                     second=0,
                     microsecond=0,
                     tzinfo=tzinfo(0, 'Z'))
        rate_datetime_lower_string = datetimetostr(rate_datetime - timedelta(minutes=5))
        rate_datetime_upper_string = datetimetostr(rate_datetime)
        rate_datetime_timestamp = calendar.timegm(rate_datetime.utctimetuple())

        for queue in queues:  # type: QueueDescription
            queue_tags = list(tags)
            queue_tags.append("queue:" + queue.name)

            self.gauge('queue.messages.total', queue.message_count, queue_tags)
            self.gauge('queue.messages.active', queue.count_details.active_message_count, queue_tags)
            self.gauge('queue.messages.scheduled', queue.count_details.scheduled_message_count, queue_tags)
            self.gauge('queue.messages.dead_lettered', queue.count_details.dead_letter_message_count, queue_tags)
            self.gauge('queue.messages.transfer', queue.count_details.transfer_message_count, queue_tags)
            self.gauge('queue.messages.transfer_dead_lettered', queue.count_details.transfer_dead_letter_message_count, queue_tags)

            for metric in sbms.get_supported_metrics_queue(namespace, queue.name):  # type: MetricProperties
                print(metric.display_name, metric.name, metric.primary_aggregation, metric.unit)

                aggregated_value = 0

                metric_datas = sbms.get_metrics_data_queue(
                    namespace, queue.name,
                    metric.name, 'PT5M',
                    '$filter=Timestamp ge datetime\'%s\' and Timestamp lt datetime\'%s\'' % (rate_datetime_lower_string, rate_datetime_upper_string))

                if len(metric_datas) > 0:
                    metric_data = metric_datas.pop()

                    aggregated_value = metric_data.total

                    if metric.primary_aggregation == "Max":
                        aggregated_value = metric_data.max

                self.gauge('queue.metrics.%s' % metric.name, aggregated_value,
                           queue_tags, timestamp=rate_datetime_timestamp)


    @staticmethod
    def _load_conf(instance):
        if 'publish_settings' in instance:
            cert_file_handle, cert_file_path = mkstemp()
            get_certificate_from_publish_settings(instance.get('publish_settings'), cert_file_path)
        else:
            cert_file_path = instance.get('cert_file')

        namespace = instance.get('namespace')

        tags = []
        if 'tags' in instance:
            tags = instance.get('tags')

        return instance.get('subscription_id'), cert_file_path, namespace, tags


if __name__ == '__main__':
    check, instances = AzureServiceBusCheck.from_yaml('AzureServiceBusCheck.yaml')
    for instance in instances:
        print "\nRunning the check against subscription: %s" % (instance['subscription_id'])
        check.check(instance)
        if check.has_events():
            print 'Events: %s' % (check.get_events())
        print 'Metrics: %s' % (check.get_metrics())