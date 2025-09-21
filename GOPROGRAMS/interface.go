package main

import "fmt"

type MotorVehicle interface {
	Mileage() float64
}

type BMW struct {
	distance     float64
	fuel         float64
	averagespeed string
}

type Audi struct {
	distance float64
	fuel     float64
}

func (b BMW) Mileage() float64 {
	return b.distance / b.fuel
}

func (a Audi) Mileage() float64 {
	return a.distance / a.fuel
}

func totalMileage(m []MotorVehicle) {
	tm := 0.0
	for _, v := range m {
		tm = tm + v.Mileage()
	}
	fmt.Printf("Total Mileage per month %f km/l\n", tm)
}

func main() {
	b1 := BMW{
		distance:     167.9,
		fuel:         36,
		averagespeed: "58",
	}

	a1 := Audi{
		distance: 152.9,
		fuel:     30,
	}

	person := []MotorVehicle{b1, a1}
	totalMileage(person)
}