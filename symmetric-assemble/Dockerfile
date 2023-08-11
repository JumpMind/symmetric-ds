FROM openjdk:8-jdk

# Input the server zip file used to install SymmetricDS
# This file must be in the Docker context (i.e. working directory) 

WORKDIR /app
RUN curl -L -o temp.zip https://downloads.sourceforge.net/project/symmetricds/symmetricds/symmetricds-3.14/symmetric-server-3.14.4.zip?ts=gAAAAABk1iEN9Flx5PwJunCKTSS6A_KnSLcS9V-HmnPeEtBqsQmuzgvUrbWmYpGomzGFSsWiAJK35T8q_bSeGi9AyFtOk058uw%3D%3D&r=https%3A%2F%2Fsourceforge.net%2Fprojects%2Fsymmetricds%2Ffiles%2Fsymmetricds%2Fsymmetricds-3.14%2Fsymmetric-server-3.14.4.zip%2Fdownload

COPY temp.zip /app/symmetric-ds.zip

# Install SymmetricDS
RUN unzip /app/symmetric-ds.zip
RUN rm -f /app/symmetric-ds.zip
RUN mkdir -p /opt/
RUN mv symmetric-server* /opt/symmetric-ds
RUN chmod -R u=rwX,g=,o= /opt/symmetric-ds/security

ENV SYM_HOME /opt/symmetric-ds

# Create a volume containing the engines, tmp, conf, and security directories
VOLUME /opt/symmetric-ds/engines
VOLUME /opt/symmetric-ds/tmp
VOLUME /opt/symmetric-ds/conf
VOLUME /opt/symmetric-ds/security

EXPOSE 31415
EXPOSE 31417

CMD /opt/symmetric-ds/bin/sym_service start && tail -F /opt/symmetric-ds/logs/symmetric.log