
data = read.csv("~/Repositories/coheel-stratosphere/plots/performance_feature_join/data.csv",
                header = TRUE,
                sep = ";")

plot(data$Documents, data$Minutes,
     xlab = "# Documents",
     ylab = "Runtime in Minutes",
     main = "Runtime")

# plot(data$Parallelism, data$Speedup, xlab = "Parallelism", ylab = "Speedup", main = "Relative Speedup Factor", xaxt = "n")
# axis(1, at = seq(1, 10, by = 1), las=2)
# # Draw y = x
# lines(seq(1, 10), col = "blue", lwd = 3)
# # Draw actual speedup
# m = lm(data$Speedup ~ data$Parallelism)
# abline(m, lwd = 3, col = "#006400")
# m