# ------- build the bin ------------
FROM golang:1.11.2-stretch
WORKDIR /go/src/github.com/ContextLogic/eventsum
COPY . /go/src/github.com/ContextLogic/eventsum

# install go dep
RUN curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh

# install deps
RUN dep ensure

# build the bin
RUN CGO_ENABLED=0 GOOS=linux go build -o /go/bin/eventsum -a -installsuffix cgo ./cmd/example.go


# ------- run the bin ------------
FROM alpine:3.7
WORKDIR /root/
COPY --from=0 /go/bin/eventsum .

# add EventSum config file
RUN mkdir /root/config
ADD config/datasourceinstance.yaml.prod /root/config/datasourceinstance.yaml
ADD config/config.json /root/config
ADD config/logconfig.json /root/config
ADD config/schema.json /root/config

# run
CMD /root/eventsum -c /root/config/config.json