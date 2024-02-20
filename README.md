# Gengo

Gengo is a command-line interface (CLI) tool designed to generate large fake datasets quickly. 
It enables users to simulate massive amounts of data for testing, development, or educational purposes. 
With Gengo, you can effortlessly create datasets containing hundreds of millions of rows in just a matter of minutes.

## Installation

To install Gengo, follow these steps:

```
git clone https://github.com/peekknuf/Gengo.git
cd Gengo
go build
./Gengo gen
```

You will be prompted to enter the number of rows and the name of the output file. After providing the required inputs, the program will generate the dataset and save it to the specified CSV file.(csv only for now)

You can always change the code according to your specific needs, after all gofakeit is very straighforward, or if you don't really care just leave it as is