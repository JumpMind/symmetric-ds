################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/db/platform/sqlite/SqliteDdlReader.c \
../src/db/platform/sqlite/SqlitePlatform.c \
../src/db/platform/sqlite/SqliteSqlTemplate.c \
../src/db/platform/sqlite/SqliteSqlTransaction.c 

OBJS += \
./src/db/platform/sqlite/SqliteDdlReader.o \
./src/db/platform/sqlite/SqlitePlatform.o \
./src/db/platform/sqlite/SqliteSqlTemplate.o \
./src/db/platform/sqlite/SqliteSqlTransaction.o 

C_DEPS += \
./src/db/platform/sqlite/SqliteDdlReader.d \
./src/db/platform/sqlite/SqlitePlatform.d \
./src/db/platform/sqlite/SqliteSqlTemplate.d \
./src/db/platform/sqlite/SqliteSqlTransaction.d 


# Each subdirectory must supply rules for building sources it contributes
src/db/platform/sqlite/%.o: ../src/db/platform/sqlite/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/elong/git/3.7/symmetric-ds/symmetric-client-clib/inc" -O0 -g3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


