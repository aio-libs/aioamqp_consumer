clean:
	find aioamqp_consumer -type d -name "__pycache__" -exec rm -rf {} + > /dev/null 2>&1
	find aioamqp_consumer -type f -name "*.pyc" -exec rm -rf {} + > /dev/null 2>&1

	find tests -type d -name "__pycache__" -exec rm -rf {} + > /dev/null 2>&1
	find tests -type f -name "*.pyc" -exec rm -rf {} + > /dev/null 2>&1

lint:
	flake8 --show-source aioamqp_consumer
	isort --check-only -rc aioamqp_consumer --diff

	flake8 --show-source setup.py
	isort --check-only setup.py --diff

	flake8 --show-source tests
	isort --check-only -rc tests --diff

test:
	pytest tests

install:
	pip install -r requirements/ci.txt
	pip install -e .

all: clean lint test
