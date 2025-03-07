resource "aws_sfn_state_machine" "pipeline_machine" {
  name     = "pipeline-state-machine"
  role_arn = aws_iam_role.state_machine_role.arn

  definition = jsonencode({
  "StartAt": "Ingest Lambda",
  "States": {
    "Ingest Lambda": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "Payload": {
          "timestamp": "sampleValue1",
          "uuid": "uniquenumber",
          "fact_tables": [
            "sales_order"
          ],
          "dim_tables":[
            "design",
            "currency",
            "staff",
            "counterparty",
            "address",
            "department",
            "purchase_order",
            "payment_type"
          ]
        },
        "FunctionName": "ingestion_handler"
      },
      "Retry": [
        {
          "ErrorEquals": [
            ""
          ],
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL",
          "IntervalSeconds": 60
        }
      ],
      "Next": "Transform Lambda"
    },
    "Transform Lambda": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "Payload": {
          "timestamp": "sampleValue1",
          "uuid": "uniquenumber"
        },
        "FunctionName": "transform_handler"
      },
      "Retry": [
        {
          "ErrorEquals": [
            ""
          ],
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL",
          "IntervalSeconds": 60
        }
      ],
      "Next": "Load Lambda"
    },
    "Load Lambda": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "Payload": {
          "timestamp": "sampleValue1",
          "uuid": "uniquenumber"
        },
        "FunctionName": "load_lambda" #update me
      },
      "Retry": [
        {
          "ErrorEquals": [
            ""
          ],
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL",
          "IntervalSeconds": 60
        }
      ],
      "End": true
    }
  }
})

  logging_configuration {
    level = "ALL"
    include_execution_data = true
    log_destination = "${aws_cloudwatch_log_group.state_machine_logs.arn}:*"
  }
  depends_on = [ aws_cloudwatch_log_group.state_machine_logs, aws_iam_role_policy_attachment.state_machine_logs_attachment ]
}