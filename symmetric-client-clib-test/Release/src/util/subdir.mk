################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/util/PropertiesTest.c \
../src/util/StringBuilderTest.c 

OBJS += \
./src/util/PropertiesTest.o \
./src/util/StringBuilderTest.o 

C_DEPS += \
./src/util/PropertiesTest.d \
./src/util/StringBuilderTest.d 


# Each subdirectory must supply rules for building sources it contributes
src/util/%.o: ../src/util/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/elong/workspace-cdt/symmetric-client-clib-test/inc" -I"/home/elong/workspace-cdt/symmetric-client-clib/inc" -O3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


