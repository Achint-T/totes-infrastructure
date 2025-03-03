# resource "aws_cloudwatch_log_metric_filter" "error_filter" {
#   name           = "ErrorFilter"
#   log_group_name = "/aws/state-machine/pipeline" #update for the actual log group name
#   pattern        = "ExecutionFailed" # Adjust pattern to match error logs 

#   metric_transformation {
#     name          = "ErrorCount"
#     namespace     = "LogMetrics"
#     value         = "1"
#     default_value = 0
#   }
#   depends_on = [ aws_cloudwatch_log_group.state_machine_logs ]
# }

# resource "aws_cloudwatch_metric_alarm" "error_alarm_with_role" {
#   alarm_name          = "ErrorAlarm"
#   comparison_operator = "GreaterThanOrEqualToThreshold"
#   evaluation_periods  = "1"
#   metric_name         = "ErrorCount" # Metric name is defined here
#   namespace           = "LogMetrics"
#   period              = "60"
#   statistic           = "Sum"
#   threshold           = "1"
#   alarm_actions       = [aws_sns_topic.error_notifications.arn]
#   depends_on          = [aws_cloudwatch_log_metric_filter.error_filter, aws_sns_topic_subscription.email_subscription, aws_iam_role_policy_attachment.cloudwatch_alarm_sns_policy_attachment]
# }

# Alarm for extract lambda

resource "aws_cloudwatch_log_metric_filter" "error_filter_extract" {
  name           = "ErrorFilterExtract"
  log_group_name = "/aws/state-machine/pipeline" #update for the actual log group name
  pattern        = "ExecutionFailed" # Adjust pattern to match error logs 

  metric_transformation {
    name          = "ErrorCountExtract"
    namespace     = "LogMetrics"
    value         = "1"
    default_value = 0
  }
  depends_on = [ aws_cloudwatch_log_group.state_machine_logs ]
}

resource "aws_cloudwatch_metric_alarm" "error_alarm_extract" {
  alarm_name          = "ErrorAlarmExtract"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "ErrorCountExtract" # Metric name is defined here
  namespace           = "LogMetrics"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Alarm triggered by Extract Lambda"
  alarm_actions       = [aws_sns_topic.error_notifications.arn]
  depends_on          = [aws_cloudwatch_log_metric_filter.error_filter_extract, aws_sns_topic_subscription.email_subscription, aws_iam_role_policy_attachment.cloudwatch_alarm_sns_policy_attachment]
}

# Alarm for transform lambda

resource "aws_cloudwatch_log_metric_filter" "error_filter_transform" {
  name           = "ErrorFilterTransform"
  log_group_name = "/aws/state-machine/pipeline" #update for the actual log group name
  pattern        = "ExecutionFailed" # Adjust pattern to match error logs 

  metric_transformation {
    name          = "ErrorCountTransform"
    namespace     = "LogMetrics"
    value         = "1"
    default_value = 0
  }
  depends_on = [ aws_cloudwatch_log_group.state_machine_logs ]
}

resource "aws_cloudwatch_metric_alarm" "error_alarm_transform" {
  alarm_name          = "ErrorAlarmTransform"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "ErrorCountTransform" # Metric name is defined here
  namespace           = "LogMetrics"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Alarm triggered by Transform Lambda"
  alarm_actions       = [aws_sns_topic.error_notifications.arn]
  depends_on          = [aws_cloudwatch_log_metric_filter.error_filter_transform, aws_sns_topic_subscription.email_subscription, aws_iam_role_policy_attachment.cloudwatch_alarm_sns_policy_attachment]
}

# Alarm for load lambda

resource "aws_cloudwatch_log_metric_filter" "error_filter_load" {
  name           = "ErrorFilterLoad"
  log_group_name = "/aws/state-machine/pipeline" #update for the actual log group name
  pattern        = "ExecutionFailed" # Adjust pattern to match error logs 

  metric_transformation {
    name          = "ErrorCountLoad"
    namespace     = "LogMetrics"
    value         = "1"
    default_value = 0
  }
  depends_on = [ aws_cloudwatch_log_group.state_machine_logs ]
}

resource "aws_cloudwatch_metric_alarm" "error_alarm_load" {
  alarm_name          = "ErrorAlarmLoad"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "ErrorCountLoad" # Metric name is defined here
  namespace           = "LogMetrics"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Alarm triggered by Load Lambda"
  alarm_actions       = [aws_sns_topic.error_notifications.arn]
  depends_on          = [aws_cloudwatch_log_metric_filter.error_filter_load, aws_sns_topic_subscription.email_subscription, aws_iam_role_policy_attachment.cloudwatch_alarm_sns_policy_attachment]
}



resource "aws_cloudwatch_log_group" "state_machine_logs" {
  name = "/aws/state-machine/pipeline"
  retention_in_days = 30
  
}