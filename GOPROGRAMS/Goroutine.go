package main
import "fmt"

func display(s string) {
    for i := 0; i < 3; i++ {
        fmt.Println(s)
    }
}

func main() {
    go display("Hello, Goroutine!")  
    display("Hello, Main!")
}