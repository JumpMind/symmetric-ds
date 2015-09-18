################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/core/SymEngineTest.c 

OBJS += \
./src/core/SymEngineTest.o 

C_DEPS += \
./src/core/SymEngineTest.d 


# Each subdirectory must supply rules for building sources it contributes
src/core/%.o: ../src/core/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/elong/workspace-cdt/symmetric-client-clib-test/inc" -I"/home/elong/workspace-cdt/symmetric-client-clib/inc" -O3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


