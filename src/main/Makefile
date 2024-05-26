echo:
	echo "Hello Lab-1 Map Reduce"

plugin:
	go build -buildmode=plugin ../mrapps/wc.go

clean:
	rm -f mr-out* & rm -f coordinator.log & rm -f mr-*-*

coordinator:
	make plugin & make clean & go run mrcoordinator.go pg-*.txt

worker:
	make plugin & go run mrworker.go wc.so

test:
	zsh test-mr.sh