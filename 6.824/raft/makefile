test:
	for i in `seq 1 100`; do go test -run Rejoin -debug 1 >> log/test_$${i}.log || echo test_$${i}.log & done

check:
	grep -l FAIL log/* && grep -l warning log/* || grep -l warning log/* || echo ok

clean:
	rm  log/*

one:
	go test -run Rejoin  -debug 1
