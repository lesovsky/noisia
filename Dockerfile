# stage 1: build
FROM golang:1.25 AS build
LABEL stage=intermediate
WORKDIR /app
COPY . .
RUN make build

# stage 2: scratch
FROM scratch AS scratch
COPY --from=build /app/bin/noisia /bin/noisia
CMD ["noisia"]
