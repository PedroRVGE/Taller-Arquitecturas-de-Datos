output "instance_id" {
  value = aws_instance.this.id
}

output "public_ip" {
  value = aws_instance.this.public_ip
}

output "public_dns" {
  value = aws_instance.this.public_dns
}

output "security_group_id" {
  description = "Úsalo como origen en los security groups de las bases de datos."
  value       = aws_security_group.this.id
}

output "role_arn" {
  value = local.create_role ? aws_iam_role.this[0].arn : null
}

output "role_name" {
  value = local.create_role ? aws_iam_role.this[0].name : null
}
