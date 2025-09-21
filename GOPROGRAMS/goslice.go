package main
import "fmt"
func main(){
	fmt.Println("GO Slice Program")
	var s1=[]int{100,200}
	fmt.Println(s1)
	s1=append(s1,1,2,3,4,5)
	fmt.Println(s1)
}