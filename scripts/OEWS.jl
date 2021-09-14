# OEWS.jl
#= This script pulls the data from
- U.S. Department of Labor
  - Bureau of Labor Statistics
    - Occupational Employment and Wage Statistics
	  - National industry-specific and by ownership
	    - OCC code
		- NAICS
		- Average Annual Salary
		- Number of Employees

For the private sector.

## Source: https://www.bls.gov/oes/tables.htm
=#
using InfoZIP: InfoZIP, unzip
using Downloads: Downloads, download
using HTTP: HTTP, URI, request
using ExcelFiles: ExcelFiles, load, openxl
using XLSX: XLSX, readxlsx, readtable, sheetnames
using CSV: CSV
using DataFrames: DataFrames, DataFrame, combine, groupby, ByRow, dropmissing!, rename!,
				  transform!, select!, select, subset!, leftjoin, Not

#=
We first download the Occupational Employment and Wage Statistics data files for 2008-2019.
=#  
for yr in 08:19
	yr = lpad(yr, 2, '0')
	isfile(joinpath("data", "oews_$yr.zip")) ||
		download(string(URI(scheme = "https",
                            host = "www.bls.gov",
                            path = "/oes/special.requests/oesm$(yr)in4.zip")),
				 joinpath("data", "oews_$yr.zip"))
end
#=
Unzip 2008-2011
=#
for yr in 08:11
	yr = lpad(yr, 2, '0')
	isdir(joinpath("data", "oesm$(yr)in4")) || mkdir(joinpath("data", "oesm$(yr)in4"))
    isempty(readdir(joinpath("data", "oesm$(yr)in4"))) &&
        unzip(joinpath("data", "oews_$(yr).zip"), joinpath("data", "oesm$(yr)in4"))
end
#=
Unzip 2012-2019
=#
for yr in 12:19
    isdir(joinpath("data", "oews_$(yr).zip")) ||
		unzip(joinpath("data", "oews_$(yr).zip"), "data")
end
#=
Download the crosswalk for OCC codes from 2010 - 2019.
=#
if !isfile(joinpath("data", "oes_2019_hybrid_structure.xlsx"))
	download(string(URI(scheme = "https",
						host = "www.bls.gov",
						path = "/oes/oes_2019_hybrid_structure.xlsx")),
			 joinpath("data", "oes_2019_hybrid_structure.xlsx"))
end
#=
Download the crosswalk for OCC codes from 2000 - 2010.
=#
if !isfile(joinpath("data", "may_2010_occs.xls"))
	download(string(URI(scheme = "https",
						host = "www.bls.gov",
						path = "/oes/may_2010_occs.xls")),
			 joinpath("data", "may_2010_occs.xls"))
end
#=
Values might be available or not. This helper function handles that logic.
=#
# get_the_number(x) = get_the_number(string(x))
get_the_number(x::Number) = x
get_the_number(x::AbstractString) =
    occursin(r"^\d+\.?\d+$", x) ? parse(Int, x) : missing

"""
	parse_oews(file::AbstractString)::DataFrame

Returns the parsed data from an OEWS file.
"""
function parse_oews(file::AbstractString)
	if endswith(file, "xls")
		data = DataFrame(load(file, only(openxl(file).workbook.sheet_names())))
	elseif endswith(file, "xlsx")
		data = readxlsx(file)
        data = DataFrame(readtable(file, first(sheetnames(data)))...)
	end
	rename!(data, names(data) .=> lowercase.(names(data)))
	data[!,:year] .= parse(Int, match(r"\d{4}", file).match)
	select!(data, [:year, :occ_code, :occ_title, :naics, :tot_emp, :a_mean])
	transform!(data,
			   :a_mean => ByRow(get_the_number),
			   :tot_emp => ByRow(get_the_number),
			   renamecols = false)
end

data = DataFrame(year = Int[],
				 occ_code = String[],
				 occ_title = String[],
				 naics = String[],
				 tot_emp = Union{Missing,Int}[],
				 a_mean = Union{Missing,Int}[])
# empty!(data)
for dir in filter!(dir -> occursin(r"^oesm\d{2}in4$", dir), readdir("data"))
	for (root, dirs, files) in walkdir(joinpath("data", dir))
		yr = match(r"\d{2}", dir).match
		for file in filter!(file -> occursin("natsector_M20$(yr)_dl", file), files)
            occursin(r"\$", file) && continue # Skip opened Windows file
			println(joinpath(root, file))
			append!(data, parse_oews(joinpath(root, file)))
		end
	end
end
subset!(data, :naics => ByRow(!isequal("99")))
unique!(data)

x = subset(data, :occ_code => ByRow(isequal("15-1256")))
transform!(x, [:tot_emp, :a_mean] => ByRow(*) => :s_w)
sum(x[!,:s_w]) / sum(x[!,:tot_emp])
#=
We care about the Software Developers and Software Quality Assurance Analysts and Testers occupation.
This is a hybrid OCC code that started being used in 2019 (15-1256).
We will reconstruct the values for it using the crosswalk.
=#
cw19_18_10 = readxlsx(joinpath("data", "oes_2019_hybrid_structure.xlsx"))
cw19_18_10 = cw19_18_10["OES2019 Hybrid"]
cw19_18_10 = DataFrame(cw19_18_10["A7:I874"],
					   strip.(vec(cw19_18_10["A6:I6"])))
select!(cw19_18_10, [1, 2, 5, 7])
foreach(println, names(cw19_18_10))
rename!(cw19_18_10, [:oes19, :title, :oes18, :soc10])
#=
For 2008 - 2009 we need to use the 2000-2010 SOC crosswalk.
=#
cw10_00 = DataFrame(load(joinpath("data", "may_2010_occs.xls"),
						  "OES use of combined SOC data.!A11:F838"))
select!(cw10_00, [1, 3, 5])
rename!(cw10_00, [:oes10, :soc10, :soc00])
#=
Combining both crosswalks we get the proper one for 2000 - 2019
=#
cw = leftjoin(cw19_18_10, cw10_00, on = :soc10)
subset!(cw, :oes19 => ByRow(isequal("15-1256")))
cw = sort!(unique!(reduce(vcat, eachcol(select(cw, Not(:title))))))
#=
Since it is a single OCC code we can filter the records and aggregate easily.
=#
subset!(data, :occ_code => ByRow(∈(cw)))
#=
In case there are missing values, we can omit those and compute the mean based on the available data. 
=#
dropmissing!(data)
#=
We must compute the tot_emp and a_mean.
=#
transform!(data, [:tot_emp, :a_mean] => ByRow(*) => :salary_and_wages)
select!(data, [:year, :naics, :tot_emp, :salary_and_wages])
data = combine(groupby(data, [:year, :naics]),
               [:tot_emp, :salary_and_wages] .=> sum,
               renamecols = false)
transform!(data, [:salary_and_wages, :tot_emp] => ByRow(/) => :a_mean)
select!(data, [:year, :naics, :tot_emp, :a_mean])
sort!(data)
CSV.write(joinpath("data", "oews_15-1256.csv"), data)
