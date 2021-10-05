package db

type EventType string

const CPUHoursAdd EventType = "cpu.hours.add"
const CPUHoursSubtract EventType = "cpu.hours.subtract"
const CPUHoursReset EventType = "cpu.hours.reset"
const CPUHoursCalculate EventType = "cpu.hours.calculate"
