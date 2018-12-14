MAKEFLAGS += -s
SUBDIRS := $(wildcard */.)
SUBDIRS := $(filter-out __pycache__/., $(SUBDIRS))
.PHONY: clean check test $(SUBDIRS)

all : $(SUBDIRS)

clean:
	@echo "Cleaning files in $(shell basename $(CURDIR))";
	rm -f *.pyc *.pyo *~ *.log ; \

	@for d in $(SUBDIRS); do \
		$(MAKE) -C $$d clean; \
	done;

check:
	@echo "Checking syntax in $(shell basename $(CURDIR))";
	for f in *.py ; \
	do \
		len=`echo "$${#f} + 2" | bc`; \
		echo ""; \
		echo "==== File: $$f ===="; \
		pylint $$f ; \
		echo "====   ====";\
		echo ""; \
	done;
	@for d in $(SUBDIRS); do \
		$(MAKE) -C $$d check; \
	done;


test:
	if [ -e tests.py ]; \
	then \
		echo "Running tests in $(shell basename $(CURDIR))"; \
		echo ""; \
		echo "==== Test Results ===="; \
		python tests.py -v ;\
		echo "====  ===="; \
		echo ""; \
	else \
		echo "Skipping tests in $(shell basename $(CURDIR)). No test script."; \
	fi;
	@for d in $(SUBDIRS); do \
		$(MAKE) -C $$d test; \
	done;


