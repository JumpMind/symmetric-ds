################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/service/AcknowledgeService.c \
../src/service/ConfigurationService.c \
../src/service/DataExtractorService.c \
../src/service/DataLoaderService.c \
../src/service/IncomingBatchService.c \
../src/service/NodeService.c \
../src/service/OutgoingBatchService.c \
../src/service/ParameterService.c \
../src/service/PullService.c \
../src/service/PurgeService.c \
../src/service/PushService.c \
../src/service/RegistrationService.c \
../src/service/RouterService.c \
../src/service/SequenceService.c \
../src/service/TriggerRouterService.c 

OBJS += \
./src/service/AcknowledgeService.o \
./src/service/ConfigurationService.o \
./src/service/DataExtractorService.o \
./src/service/DataLoaderService.o \
./src/service/IncomingBatchService.o \
./src/service/NodeService.o \
./src/service/OutgoingBatchService.o \
./src/service/ParameterService.o \
./src/service/PullService.o \
./src/service/PurgeService.o \
./src/service/PushService.o \
./src/service/RegistrationService.o \
./src/service/RouterService.o \
./src/service/SequenceService.o \
./src/service/TriggerRouterService.o 

C_DEPS += \
./src/service/AcknowledgeService.d \
./src/service/ConfigurationService.d \
./src/service/DataExtractorService.d \
./src/service/DataLoaderService.d \
./src/service/IncomingBatchService.d \
./src/service/NodeService.d \
./src/service/OutgoingBatchService.d \
./src/service/ParameterService.d \
./src/service/PullService.d \
./src/service/PurgeService.d \
./src/service/PushService.d \
./src/service/RegistrationService.d \
./src/service/RouterService.d \
./src/service/SequenceService.d \
./src/service/TriggerRouterService.d 


# Each subdirectory must supply rules for building sources it contributes
src/service/%.o: ../src/service/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/elong/workspace-cdt/symmetric-client-clib/inc" -O3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


