################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/db/platform/DatabasePlatform.c \
../src/db/platform/DatabasePlatformFactory.c \
../src/db/platform/DdlReader.c 

OBJS += \
./src/db/platform/DatabasePlatform.o \
./src/db/platform/DatabasePlatformFactory.o \
./src/db/platform/DdlReader.o 

C_DEPS += \
./src/db/platform/DatabasePlatform.d \
./src/db/platform/DatabasePlatformFactory.d \
./src/db/platform/DdlReader.d 


# Each subdirectory must supply rules for building sources it contributes
src/db/platform/%.o: ../src/db/platform/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/elong/git/3.7/symmetric-ds/symmetric-client-clib/inc" -O0 -g3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


