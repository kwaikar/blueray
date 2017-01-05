val filePath="hdfs://localhost/user/user_small.csv";
var inputFile = sc.textFile(filePath)
println(inputFile.collect().size + " =======> " + inputFile.collect().mkString);
