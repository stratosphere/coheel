
data = read.csv("~/Repositories/coheel-stratosphere/plots/performance_wiki/data.csv",
                header = TRUE,
                sep = ";")

data = data[data$Parallelism != 20, ]
baseTime = data$Minutes[data$Parallelism == 1]
data$Speedup = baseTime / data$Minutes

plot(data$Parallelism, data$Minutes, xlab = "Parallelism", ylab = "Runtime in Minutes", main = "Runtime")
plot(data$Parallelism, data$Speedup, xlab = "Parallelism", ylab = "Speedup", main = "Relative Speedup Factor", xaxt = "n")
axis(1, at = seq(1, 10, by = 1), las=2)
# Draw y = x
lines(seq(1, 10), col = "blue", lwd = 3)
# Draw actual speedup
m = lm(data$Speedup ~ data$Parallelism)
abline(m, lwd = 3, col = "#006400")
m