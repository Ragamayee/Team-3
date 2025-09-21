package main
import "time"

func main() {
    sem := make(chan struct{}, 5) // allow only 5 goroutines at a time

    for i := 0; i < 20; i++ {
        sem <- struct{}{} // acquire slot
        go func(id int) {
            defer func() { <-sem }() // release slot
            time.Sleep(1 * time.Second)
        }(i)
    }

    time.Sleep(3 * time.Second) // wait (simplified)
}