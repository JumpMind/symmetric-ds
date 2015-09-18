################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/model/IncomingBatch.c \
../src/model/Node.c \
../src/model/NodeSecurity.c \
../src/model/RemoteNodeStatus.c 

OBJS += \
./src/model/IncomingBatch.o \
./src/model/Node.o \
./src/model/NodeSecurity.o \
./src/model/RemoteNodeStatus.o 

C_DEPS += \
./src/model/IncomingBatch.d \
./src/model/Node.d \
./src/model/NodeSecurity.d \
./src/model/RemoteNodeStatus.d 


# Each subdirectory must supply rules for building sources it contributes
src/model/%.o: ../src/model/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/elong/git/3.7/symmetric-ds/symmetric-client-clib/inc" -O0 -g3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


