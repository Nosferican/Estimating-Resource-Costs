using CSV, DataFrames
#=
Load the Salary and Wages data and Components of Value Added data
=#
oews = CSV.read(joinpath("data", "oews_15-1256.csv"), DataFrame)
cva = CSV.read(joinpath("data", "resource_cost_factors.csv"), DataFrame)
#=
Join the data
=#
resource_cost = leftjoin(oews, cva, on = [:year, :naics])
dropmissing!(resource_cost) # Drop NAICS 99 since OEWS does not cover it
#=
Compute the resource cost by multiplying
- number of employees
- average annual salary (or wage equivalent)
- blow-up factor (value added / wages and salary)
=#
transform!(resource_cost, [:tot_emp, :a_mean, :y_w] => ByRow(*) => :resource_cost)
#=
For sum everything up by year
=#
resource_cost = combine(groupby(resource_cost, :year),
                        [:tot_emp, :resource_cost] .=> sum,
                        renamecols = false)
#=
Compute the per employee average resource cost
=#
transform!(resource_cost, [:resource_cost, :tot_emp] => ByRow(/) => :arc)
output = select(resource_cost, [:year, :arc])
#=
Divide by 12 to go from average annual resource cost per employee to person month resource cost
=#
transform!(output, :arc => ByRow(x -> x / 12) => :person_monthly_resource_cost)
select!(output, [:year, :person_monthly_resource_cost])
#=
Save the final result 🎊
=#
CSV.write(joinpath("data", "person_monthly_resource_cost.csv"), output)
