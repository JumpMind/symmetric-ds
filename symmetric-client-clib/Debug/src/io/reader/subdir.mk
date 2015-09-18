################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/io/reader/ProtocolDataReader.c 

OBJS += \
./src/io/reader/ProtocolDataReader.o 

C_DEPS += \
./src/io/reader/ProtocolDataReader.d 


# Each subdirectory must supply rules for building sources it contributes
src/io/reader/%.o: ../src/io/reader/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/elong/git/3.7/symmetric-ds/symmetric-client-clib/inc" -O0 -g3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


