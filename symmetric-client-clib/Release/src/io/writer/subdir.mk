################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/io/writer/DataWriter.c \
../src/io/writer/DefaultDatabaseWriter.c 

OBJS += \
./src/io/writer/DataWriter.o \
./src/io/writer/DefaultDatabaseWriter.o 

C_DEPS += \
./src/io/writer/DataWriter.d \
./src/io/writer/DefaultDatabaseWriter.d 


# Each subdirectory must supply rules for building sources it contributes
src/io/writer/%.o: ../src/io/writer/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/elong/workspace-cdt/symmetric-client-clib/inc" -O3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


