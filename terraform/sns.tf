resource "aws_sns_topic" "error_notifications" {
  name = "cloudwatch-error-notifications"
}

resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.error_notifications.arn
  protocol  = "email"
  endpoint  = "your-email@example.com" # Replace email address when decided
}

resource "aws_cloudwatch_metric_alarm" "error_alarm" {
  alarm_name                = "ErrorAlarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "ErrorCount"
  namespace                 = "LogMetrics"
  period                    = "60" #reconsider
  statistic                 = "Sum"
  threshold                 = "1"
  alarm_description         = "Alarm when ErrorCount metric is >= 1"
  alarm_actions             = [aws_sns_topic.error_notifications.arn]
  depends_on                = [aws_cloudwatch_log_metric_filter.error_filter, aws_sns_topic_subscription.email_subscription] #
}