using CSV, DataFrames
#=
Load the Salary and Wages data and Components of Value Added data
=#
oews = CSV.read(joinpath("data", "oews_15-1256.csv"), DataFrame)
cva = CSV.read(joinpath("data", "comp_of_va.csv"), DataFrame)
bls = CSV.read(joinpath("data", "bls_salary_wages_to_total_compensation.csv"), DataFrame)
usetbl = CSV.read(joinpath("data", "usetbl5415.csv"), DataFrame)
klems = CSV.read(joinpath("data", "klems.csv"), DataFrame)

#=
This is the approach based on Carol.
=#
car_sw = transform(oews, [:tot_emp, :a_mean] => ByRow(*) => :s_w)
car_sw = combine(groupby(car_sw, :year),
                 [:tot_emp, :s_w] .=> sum,
                 renamecols = false)
select!(car_sw, :year => identity, [:s_w, :tot_emp] => ByRow(/) => :s_w, renamecols = false)
car = innerjoin(car_sw, bls, on = :year)
select!(car,
        :year => identity,
        [:s_w, :salary_to_total_compensation] => ByRow(*) => :coe,
        renamecols = false)
car = innerjoin(car, usetbl, on = :year)
transform!(car,
           [:coe, :ti_coe] => ByRow(*) => :ti,
           [:coe, :gos_coe] => ByRow(*) => :gos,
           [:coe, :top_coe] => ByRow(*) => :top)
select!(car,
        :year => identity,
        [:coe, :ti, :gos, :top] => ByRow(+) => :car,
        renamecols = false)
transform!(car, :car => ByRow(x -> x / 12), renamecols = false)
#=
Alternative Approach
=#
labor = innerjoin(oews, cva, on = [:year, :naics])
employment = combine(groupby(labor, :year), :tot_emp => sum, renamecols = false)
select!(labor,
        [:year, :naics] .=> identity,
        [:tot_emp, :a_mean, :l_w] => ByRow(*) => :coe, renamecols = false)
jbsc = innerjoin(labor, klems, on = [:year, :naics])
transform!(jbsc,
           [:coe, :top_coe] => ByRow((coe, top_coe) -> coe * max(top_coe, 0)) => :top,
           [:coe, :enepsi_coe] => ByRow(*) => :ii,
           [:coe, :gos_coe] => ByRow(*) => :gos)
transform!(jbsc, [:coe, :gos, :top, :ii] => ByRow(+) => :y)
jbsc = combine(groupby(jbsc, :year), :y => sum, renamecols = false)
jbsc = innerjoin(jbsc, employment, on = :year)
select!(jbsc,
        :year => identity,
        [:y, :tot_emp] => ByRow((y, tot_emp) -> y / tot_emp / 12) => :jbsc,
        renamecols = false)

resource_cost = innerjoin(car, jbsc, on = :year)
transform!(resource_cost,
           [:car, :jbsc] .=> ByRow(x -> round(Int, x)),
           renamecols = false)
#=
Save the final result 🎊
=#
CSV.write(joinpath("data", "person_monthly_resource_cost.csv"), resource_cost)
