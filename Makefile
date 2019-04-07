include .aws_service_settings

venv: requirements.txt
	rm -rf venv
	virtualenv -p python3.6 venv
	venv/bin/pip install -r requirements.txt

.PHONY: test
test: venv
	venv/bin/pytest tests

.PHONY: deploy
deploy: clean
	zip -r deploy.zip lambda_function.py
	aws lambda update-function-code \
	--function-name $(ALEXA_FUNC_NAME) \
	--region $(ALEXA_REGION) \
	--zip-file 'fileb://deploy.zip'

.PHONY: clean
clean:
	rm -rf venv
	rm -f deploy.zip
