FROM debian:stable-slim

COPY bkdtree.test /usr/bin

ENTRYPOINT [ "/usr/bin/bkdtree.test" ]
