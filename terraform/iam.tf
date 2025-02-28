#ingestion lambda iam role, permissions. some parts reusable for other lambdas: 
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

resource "aws_iam_role" "lambda_role" {
  name_prefix        = "role-ingestion-lambdas-"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

data "aws_iam_policy_document" "s3_document" {
  statement {
    actions = ["s3:PutObject", "s3:GetObject"]
    resources = [
      "${aws_s3_bucket.s3_ingestion_bucket.arn}/*", 
      "${aws_s3_bucket.s3_transform_bucket.arn}/*"
    ]
  }
}

data "aws_iam_policy_document" "cw_document" {
  statement {
    actions = ["logs:CreateLogGroup"]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
  statement {
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:*:*"
    ]
  }
}

resource "aws_iam_policy" "s3_policy" {
  name        = "s3-policy-ingestion-lambda"
  description = "policy for writing to the S3 bucket"
  policy      = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_policy" "cw_policy" {
  name        = "cw-policy-ingestion-lambda"
  policy      = data.aws_iam_policy_document.cw_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}

# IAM Role for EventBridge Scheduler to invoke state machine
resource "aws_iam_role" "scheduler_role" {
  name = "eventbridge-scheduler-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "scheduler.amazonaws.com"
        }
      },
    ]
  })
}

resource "aws_iam_policy_attachment" "scheduler_state_machine_policy_attachment" {
  name       = "scheduler-state-machine-policy-attachment"
  roles      = [aws_iam_role.scheduler_role.name]
  policy_arn = aws_iam_policy.scheduler_state_machine_policy.arn
}

resource "aws_iam_policy" "scheduler_state_machine_policy" {
  name        = "scheduler-state-machine-policy"
  description = "Policy to allow EventBridge Scheduler to invoke state machine"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = [ "states:StartExecution" ],
        Resource = aws_sfn_state_machine.pipeline_machine.arn
      },
    ]
  })
}

resource "aws_iam_role" "cloudwatch_alarm_sns_role" {
  name = "CloudWatchAlarmSNSRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Principal = {
          Service = "cloudwatch.amazonaws.com"
        }
        Effect = "Allow"
        Sid    = ""
      },
    ]
  })
}

resource "aws_iam_policy" "cloudwatch_alarm_sns_policy" {
  name        = "CloudWatchAlarmSNSPolicy"
  description = "Policy to allow CloudWatch Alarm to publish to SNS Topic"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "sns:Publish"
        ]
        Effect   = "Allow"
        Resource = aws_sns_topic.error_notifications.arn
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "cloudwatch_alarm_sns_policy_attachment" {
  role       = aws_iam_role.cloudwatch_alarm_sns_role.name
  policy_arn = aws_iam_policy.cloudwatch_alarm_sns_policy.arn
}

resource "aws_iam_role" "state_machine_role" {
  name = "state-machine-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "state_machine_cloudwatch_logs_policy" {
  name        = "state-machine-cloudwatch-logs-policy"
  description = "Policy to allow state machine to write logs to CloudWatch Logs"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          # "logs:CreateLogStream",
          # "logs:PutLogEvents"
          "*"
        ],
        Resource = [
          # "${aws_cloudwatch_log_group.state_machine_logs.arn}:*",
          # aws_cloudwatch_log_group.state_machine_logs.arn      
          "*"   
        ]
      }
    ]
  })

}

resource "aws_iam_role_policy_attachment" "state_machine_logs_attachment" {
  role       = aws_iam_role.state_machine_role.name
  policy_arn = aws_iam_policy.state_machine_cloudwatch_logs_policy.arn
}

resource "aws_iam_policy" "secretsmanager_access_policy" {
  name        = "secretsmanager-access-policy"
  description = "Policy to allow Lambda function to access Secrets Manager"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret" 
        ],
        Effect = "Allow",
        Resource = ["*"]
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_secretsmanager_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.secretsmanager_access_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_secretsmanager_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.secretsmanager_access_policy.arn
}

resource "aws_iam_policy" "step_functions_policy" {
  name        = "step-functions-policy-lambda-invoke"
  description = "Policy to allow Step Functions to invoke Lambda function"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "lambda:InvokeFunction"
        ],
        Effect   = "Allow",
        Resource = ["*"]
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "step_functions_policy_attachment" {
  role       = aws_iam_role.state_machine_role.name
  policy_arn = aws_iam_policy.step_functions_policy.arn
}