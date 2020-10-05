test:
	for i in `seq 1 30`; do go test -run BasicAgree -debug 1 >> log/test_$${i}.log || echo test_$${i}.log & done

check:
	grep -l FAIL log/* && grep -l warning log/* || grep -l warning log/* || echo ok

clean:
	rm  log/*

one:
	go test -run Backup -debug 1
