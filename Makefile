# Author:       nghiatc
# Email:        congnghia0609@gmail.com

.PHONY: deps
deps:
	@./deps.sh

.PHONY: main
main:
	@go run main.go

.PHONY: pub
pub:
	@go run pub.go

.PHONY: sub
sub:
	@go run sub.go

.PHONY: ssl
ssl:
	@cd ./ssl; ./gen_ssl.sh; cd ..;
