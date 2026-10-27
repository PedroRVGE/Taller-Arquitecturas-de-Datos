output "bucket_name" {
  value = aws_s3_bucket.data.id
}

output "airflow_instance_id" {
  value = module.airflow.instance_id
}

output "airflow_url" {
  description = "Abre la UI si tu IP está en allowed_cidrs; si no, usa el túnel de SSM (ver README)."
  value       = "http://${module.airflow.public_dns}:8080"
}

output "ssm_port_forward_command" {
  value = "aws ssm start-session --target ${module.airflow.instance_id} --document-name AWS-StartPortForwardingSession --parameters portNumber=8080,localPortNumber=8080 --region ${var.aws_region}"
}

output "secret_arn" {
  description = "Credenciales (incluida la del admin de Airflow): aws secretsmanager get-secret-value --secret-id <arn>"
  value       = aws_secretsmanager_secret.app.arn
}

output "rds_endpoint" {
  value = aws_db_instance.mysql.address
}

output "docdb_endpoint" {
  value = aws_docdb_cluster.this.endpoint
}

output "redshift_endpoint" {
  value = aws_redshiftserverless_workgroup.this.endpoint[0].address
}

output "redshift_iam_role_arn" {
  value = local.redshift_role_arn
}

output "code_version" {
  value = local.code_version
}
