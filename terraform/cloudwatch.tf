resource "aws_cloudwatch_log_metric_filter" "error_filter" {
  name           = "ErrorFilter"
  log_group_name = "/aws/state-machine/pipeline" #update for the actual log group name
  pattern        = "ExecutionFailed" # Adjust pattern to match error logs 

  metric_transformation {
    name          = "ErrorCount"
    namespace     = "LogMetrics"
    value         = "1"
    default_value = 0
  }
  depends_on = [ aws_cloudwatch_log_group.state_machine_logs ]
}

resource "aws_cloudwatch_metric_alarm" "error_alarm_with_role" {
  alarm_name          = "ErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "ErrorCount" # Metric name is defined here
  namespace           = "LogMetrics"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_actions       = [aws_sns_topic.error_notifications.arn]
  depends_on          = [aws_cloudwatch_log_metric_filter.error_filter, aws_sns_topic_subscription.email_subscription, aws_iam_role_policy_attachment.cloudwatch_alarm_sns_policy_attachment]
}

resource "aws_cloudwatch_log_group" "state_machine_logs" {
  name = "/aws/state-machine/pipeline"
  retention_in_days = 30
  
}