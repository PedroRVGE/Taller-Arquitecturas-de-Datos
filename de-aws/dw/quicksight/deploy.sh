#!/usr/bin/env bash
# Crea (o elimina) el dataset Direct Query y el dashboard de QuickSight sobre marts.rpt_sales_dashboard.
# Requisitos: suscripción a QuickSight y la fuente de datos creada por Terraform
# (quicksight_principal_arn definido y terraform apply).
#
#   ./deploy.sh create   # dataset + dashboard publicado
#   ./deploy.sh delete   # borrar antes de terraform destroy
set -euo pipefail
cd "$(dirname "$0")"

export REGION="${AWS_REGION:-us-east-1}"
export ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
export QS_USER_ARN="${QS_USER_ARN:-$(cd ../terraform && terraform output -raw quicksight_principal_arn)}"
DATASET_ID=de-aws-dw-rpt-sales
DASHBOARD_ID=de-aws-dw-ventas

render() { envsubst '${ACCOUNT_ID} ${REGION} ${QS_USER_ARN}' < "$1"; }

case "${1:-}" in
  create)
    aws quicksight create-data-set --region "$REGION" --cli-input-json "$(render dataset.json)" --query Arn --output text
    aws quicksight create-dashboard --region "$REGION" --cli-input-json "$(render dashboard.json)" --query Arn --output text
    for _ in $(seq 1 30); do
      status=$(aws quicksight describe-dashboard --region "$REGION" --aws-account-id "$ACCOUNT_ID" \
        --dashboard-id "$DASHBOARD_ID" --query Dashboard.Version.Status --output text)
      [ "$status" != CREATION_IN_PROGRESS ] && break
      sleep 5
    done
    echo "Dashboard: $status"
    echo "https://${REGION}.quicksight.aws.amazon.com/sn/dashboards/${DASHBOARD_ID}"
    ;;
  delete)
    aws quicksight delete-dashboard --region "$REGION" --aws-account-id "$ACCOUNT_ID" --dashboard-id "$DASHBOARD_ID" || true
    aws quicksight delete-data-set --region "$REGION" --aws-account-id "$ACCOUNT_ID" --data-set-id "$DATASET_ID" || true
    ;;
  *)
    echo "Uso: $0 create|delete" >&2
    exit 1
    ;;
esac
