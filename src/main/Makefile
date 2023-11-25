echo:
	echo "Hello Lab-1 Map Reduce"

plugin:
	go build -buildmode=plugin ../mrapps/wc.go

clean:
	rm mr-out*

coordinator:
	make plugin & make clean & go run mrcoordinator.go pg-*.txt

worker:
	make plugin & go run mrworker.go wc.so

test:
	zsh test-mr.sh