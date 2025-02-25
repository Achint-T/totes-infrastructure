resource "aws_scheduler_schedule" "pipeline_scheduler" {
    name = "every-5-minutes-scheduler"
    schedule_expression = "cron(*/5 * * * ? *)"
    schedule_expression_timezone = "UTC"
    description = "Trigger lambda function every 5 minutes"
    flexible_time_window {
      mode = "OFF"
    }
    target {
      arn = aws_lambda_function.ingestion_handler.arn
      role_arn = aws_iam_role.scheduler_role.arn
      retry_policy {
        maximum_event_age_in_seconds = 600 #decide time frame each run will be valid
        maximum_retry_attempts = 3 #decide retry number
      }
    #Need to add payload for timestamp and run ID for pipeline observebility  
    }

  
}

