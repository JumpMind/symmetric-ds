################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/transport/TransportManagerFactory.c 

OBJS += \
./src/transport/TransportManagerFactory.o 

C_DEPS += \
./src/transport/TransportManagerFactory.d 


# Each subdirectory must supply rules for building sources it contributes
src/transport/%.o: ../src/transport/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/elong/workspace-cdt/symmetric-client-clib/inc" -O3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


