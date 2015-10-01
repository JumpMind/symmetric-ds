################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/db/SymDialect.c \
../src/db/SymDialectFactory.c 

OBJS += \
./src/db/SymDialect.o \
./src/db/SymDialectFactory.o 

C_DEPS += \
./src/db/SymDialect.d \
./src/db/SymDialectFactory.d 


# Each subdirectory must supply rules for building sources it contributes
src/db/%.o: ../src/db/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/elong/git/3.7/symmetric-ds/symmetric-client-clib/inc" -O0 -g3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


