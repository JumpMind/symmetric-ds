FROM openjdk:alpine

WORKDIR /app
COPY README.md /app

# Install editors for easy configuration
RUN apk add --no-cache nano
RUN apk add --no-cache vim

# Install SymmetricDS
ADD https://sourceforge.net/projects/symmetricds/files/latest/download /app/symmetric-ds.zip
RUN unzip /app/symmetric-ds.zip
RUN rm -f /app/symmetric-ds.zip
RUN mkdir /opt/
RUN mv symmetric-server* /opt/symmetric-ds

ENV SYM_HOME /opt/symmetric-ds

# Create a volume containing the engines, tmp, conf, and security directories
VOLUME /opt/symmetric-ds/engines
VOLUME /opt/symmetric-ds/tmp
VOLUME /opt/symmetric-ds/conf
VOLUME /opt/symmetric-ds/security

EXPOSE 31415
EXPOSE 31417

CMD /opt/symmetric-ds/bin/sym_service start && tail -F /opt/symmetric-ds/logs/symmetric.log