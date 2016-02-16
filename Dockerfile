FROM golang:1.5.3-onbuild

ENTRYPOINT [ "/kazoo-offsets" ]

ADD kazoo-offsets /kazoo-offsets
