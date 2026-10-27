# Atajos de operación. Uso: make dw-plan, make dl-apply, make dw-tunnel, etc.
# Requiere: terraform >= 1.6, AWS CLI v2 con credenciales y el plugin de Session Manager.

PARTS := dw dl
TF := terraform

.PHONY: help fmt validate test $(foreach p,$(PARTS),$(p)-init $(p)-plan $(p)-apply $(p)-destroy $(p)-outputs $(p)-tunnel $(p)-deploy-code $(p)-logs)

help:
	@echo "Infraestructura:  make <dw|dl>-init | -plan | -apply | -outputs | -destroy"
	@echo "Operación:        make <dw|dl>-tunnel (UI de Airflow) | -deploy-code | -logs"
	@echo "Calidad:          make fmt | validate | test"

fmt:
	$(TF) fmt -recursive .

validate:
	@for part in $(PARTS); do \
		(cd $$part/terraform && $(TF) init -backend=false -input=false >/dev/null && $(TF) validate) || exit 1; \
	done

test:
	cd dw/tests && python -m pytest -q
	cd dl/tests && python -m pytest -q

define PART_TARGETS
$(1)-init:
	cd $(1)/terraform && $(TF) init

$(1)-plan:
	cd $(1)/terraform && $(TF) plan -out=$(1).tfplan

$(1)-apply:
	cd $(1)/terraform && $(TF) apply $(1).tfplan

$(1)-destroy:
	cd $(1)/terraform && $(TF) destroy

$(1)-outputs:
	cd $(1)/terraform && $(TF) output

# Túnel por SSM: la UI queda en http://localhost:8080 (dw) o :8081 (dl) sin abrir puertos.
$(1)-tunnel:
	cd $(1)/terraform && eval "$$$$($(TF) output -raw ssm_port_forward_command)"

# Publica el código actual en S3 (terraform apply) y lo despliega en la EC2 por SSM.
$(1)-deploy-code:
	cd $(1)/terraform && $(TF) apply -auto-approve -target=aws_s3_object.code -target=module.airflow.aws_s3_object.config
	cd $(1)/terraform && aws ssm send-command \
		--instance-ids "$$$$($(TF) output -raw airflow_instance_id)" \
		--document-name AWS-RunShellScript \
		--parameters commands=/usr/local/bin/de-aws-deploy \
		--comment "de-aws $(1) deploy" --output text --query Command.CommandId

$(1)-logs:
	cd $(1)/terraform && aws ssm start-session --target "$$$$($(TF) output -raw airflow_instance_id)"
endef

$(foreach part,$(PARTS),$(eval $(call PART_TARGETS,$(part))))
