################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/transport/http/HttpIncomingTransport.c \
../src/transport/http/HttpOutgoingTransport.c \
../src/transport/http/HttpTransportManager.c 

OBJS += \
./src/transport/http/HttpIncomingTransport.o \
./src/transport/http/HttpOutgoingTransport.o \
./src/transport/http/HttpTransportManager.o 

C_DEPS += \
./src/transport/http/HttpIncomingTransport.d \
./src/transport/http/HttpOutgoingTransport.d \
./src/transport/http/HttpTransportManager.d 


# Each subdirectory must supply rules for building sources it contributes
src/transport/http/%.o: ../src/transport/http/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/elong/git/3.7/symmetric-ds/symmetric-client-clib/inc" -O0 -g3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


