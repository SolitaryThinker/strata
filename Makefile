all: bib main

main: main.tex bib
	pdflatex main.tex

bib: all.bib
	pdflatex main.tex
	bibtex main
	pdflatex main
	bibtex main
	pdflatex main.tex

graph: graphs/micro.py graphs/ycsb.py graphs/compaction.py
	cd graphs; python micro.py micro-64.data ;python micro.py micro-1000.data; python compaction.py compaction-64.data; python ycsb.py ycsb-64.data; python multi.py;

clean:
	rm -rf *.log *.aux *.bbl *.pdf *.out *.ent *.blg *.dvi
