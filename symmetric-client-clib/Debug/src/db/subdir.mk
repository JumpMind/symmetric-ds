################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/db/DatabasePlatform.c \
../src/db/DatabasePlatformFactory.c \
../src/db/SqliteDialect.c \
../src/db/SqlitePlatform.c \
../src/db/SymDialect.c \
../src/db/SymDialectFactory.c 

OBJS += \
./src/db/DatabasePlatform.o \
./src/db/DatabasePlatformFactory.o \
./src/db/SqliteDialect.o \
./src/db/SqlitePlatform.o \
./src/db/SymDialect.o \
./src/db/SymDialectFactory.o 

C_DEPS += \
./src/db/DatabasePlatform.d \
./src/db/DatabasePlatformFactory.d \
./src/db/SqliteDialect.d \
./src/db/SqlitePlatform.d \
./src/db/SymDialect.d \
./src/db/SymDialectFactory.d 


# Each subdirectory must supply rules for building sources it contributes
src/db/%.o: ../src/db/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/elong/git/3.7/symmetric-ds/symmetric-client-clib/inc" -O0 -g3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


