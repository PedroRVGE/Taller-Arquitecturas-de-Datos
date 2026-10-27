output "bucket_name" {
  value = aws_s3_bucket.lake.id
}

output "glue_database" {
  value = aws_glue_catalog_database.lake.name
}

output "glue_tables" {
  value = sort([for table in aws_glue_catalog_table.lake : table.name])
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lake.name
}

output "airflow_instance_id" {
  value = module.airflow.instance_id
}

output "airflow_url" {
  value = "http://${module.airflow.public_dns}:8080"
}

output "ssm_port_forward_command" {
  value = "aws ssm start-session --target ${module.airflow.instance_id} --document-name AWS-StartPortForwardingSession --parameters portNumber=8080,localPortNumber=8081 --region ${var.aws_region}"
}

output "secret_arn" {
  value = aws_secretsmanager_secret.app.arn
}

output "code_version" {
  value = local.code_version
}
