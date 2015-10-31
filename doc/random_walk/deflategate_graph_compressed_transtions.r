input <- "/home/knub/Repositories/coheel-stratosphere/doc/random_walk/deflategate_graph_compressed_transtions.tsv"
# alternative:
#input <- file.choose()
N <- data.matrix(read.csv(input, header = F, sep = "\t"))

# normalize row-wise
for( i in 1:length(N[1,]) ) { N[i,] <- N[i,]/sum(N[i,]) }

# start vector (assuming seed entity at position one)
s <- c(1,0,0,0,0,0,0)
alpha <- .15

norm_vec <- function(x) sqrt(sum(x^2))

r <- s;
for( i in 1:100) {
  q<-(1-alpha)*r %*% N + alpha*s;
  #message(i,": ",(r-q),"  ->  ",norm_vec(r-q));
  if(10^-8>=norm_vec(r-q)) { break; };
  r <- q
}

r

