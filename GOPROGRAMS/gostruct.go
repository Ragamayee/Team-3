package main

import "fmt"

type emp struct {
	name   string
	salary float64
	age    int
}

func main() {
	var emp1 emp
	var emp2 emp

	emp1.name = "bhargav"
	emp1.salary = 30000
	emp1.age = 24

	fmt.Println("emp1 name   :", emp1.name)
	fmt.Println("emp1 age    :", emp1.age)
	fmt.Println("emp1 salary :", emp1.salary)

	emp2.name = "siva"
	emp2.salary = 35000
	emp2.age = 30

	fmt.Println("emp2 name   :", emp2.name)
	fmt.Println("emp2 age    :", emp2.age)
	fmt.Println("emp2 salary :", emp2.salary)
}