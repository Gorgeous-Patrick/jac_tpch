FROM texlive/texlive:latest

WORKDIR /paper

COPY *.tex /paper/

CMD ["latexmk", "-pdf", "-interaction=nonstopmode", "main.tex"]
