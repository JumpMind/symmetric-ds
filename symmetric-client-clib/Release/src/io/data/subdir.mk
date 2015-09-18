################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/io/data/Batch.c \
../src/io/data/CsvData.c \
../src/io/data/DataProcessor.c 

OBJS += \
./src/io/data/Batch.o \
./src/io/data/CsvData.o \
./src/io/data/DataProcessor.o 

C_DEPS += \
./src/io/data/Batch.d \
./src/io/data/CsvData.d \
./src/io/data/DataProcessor.d 


# Each subdirectory must supply rules for building sources it contributes
src/io/data/%.o: ../src/io/data/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/elong/workspace-cdt/symmetric-client-clib/inc" -O3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


