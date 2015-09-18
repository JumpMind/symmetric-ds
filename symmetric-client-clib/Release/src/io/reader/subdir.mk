################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/io/reader/DataReader.c \
../src/io/reader/ProtocolDataReader.c 

OBJS += \
./src/io/reader/DataReader.o \
./src/io/reader/ProtocolDataReader.o 

C_DEPS += \
./src/io/reader/DataReader.d \
./src/io/reader/ProtocolDataReader.d 


# Each subdirectory must supply rules for building sources it contributes
src/io/reader/%.o: ../src/io/reader/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/elong/workspace-cdt/symmetric-client-clib/inc" -O3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


