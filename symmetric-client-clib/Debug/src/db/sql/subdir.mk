################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/db/sql/DmlStatement.c \
../src/db/sql/Row.c 

OBJS += \
./src/db/sql/DmlStatement.o \
./src/db/sql/Row.o 

C_DEPS += \
./src/db/sql/DmlStatement.d \
./src/db/sql/Row.d 


# Each subdirectory must supply rules for building sources it contributes
src/db/sql/%.o: ../src/db/sql/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/elong/git/3.7/symmetric-ds/symmetric-client-clib/inc" -O0 -g3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


